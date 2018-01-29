//------------------------------------------------------------------------------
// <copyright file="SiteRequestDispatcher.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Web.Hosting.Tracing;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public abstract class SiteRequestDispatcher
    {
        // All time are in ticks, which are equivalent to milliseconds, but are cheaper to calculate
        internal const uint BusyStatusDuration = 2000;
        private const uint QuestionableRequestLatency = 2000;
        private const uint WorkerExpirationDelay = 40000;
        private const uint WorkerExpirationCheckInterval = 5000;
        private const uint WorkerMetricsTraceInterval = 10000;
        private const uint PingInterval = 5000;
        private const int MaxLockTime = 1000;
        private const int QuestionablePendingRequestCount = 50;

        private readonly int burstLimit;
        private readonly ConcurrentDictionary<string, SiteMetadata> knownSites;
        
        public SiteRequestDispatcher(int burstLimit)
        {
            this.burstLimit = burstLimit;
            this.knownSites = new ConcurrentDictionary<string, SiteMetadata>(StringComparer.OrdinalIgnoreCase);
        }

        public virtual async Task<string> DispatchRequestAsync(
            string requestId,
            string siteName,
            string defaultHostName,
            string[] knownWorkers)
        {
            SiteMetadata site = this.knownSites.GetOrAdd(siteName, name => new SiteMetadata(siteName));

            this.UpdateWorkerList(site, knownWorkers);

            InstanceEndpoint bestInstance = site.Endpoints.ReserveBestInstance(null);
            if (site.IsBurstMode)
            {
                if (bestInstance.PendingRequestCount <= 1)
                {
                    // Endpoint is idle - choose it.
                    return bestInstance.IPAddress;
                }
                else
                {
                    // All endpoints are occupied; try to scale out.
                    using (var scaleLock = await site.ScaleLock.LockAsync(MaxLockTime))
                    {
                        if (scaleLock.TimedOut)
                        {
                            // The caller should return 429 to avoid overloading the current role
                            Debug.WriteLine("Timed-out waiting to start a scale operation in burst mode.");
                            AntaresEventProvider.EventWriteLBHttpDispatchSiteWarningMessage(site.Name, "DispatchRequest", string.Format("Timed-out ({0}ms) waiting to start a scale operation in burst mode. Reverting to {1}.", MaxLockTime, bestInstance.IPAddress));
                            return bestInstance.IPAddress;
                        }

                        if (site.IsBurstMode)
                        {
                            // No instances are idle, scale-out and send the request to the new instance.
                            // Note that there will be a cold-start penalty for this request, but it's
                            // been decided that this is better than sending a request to an existing
                            // but potentially CPU-pegged instance.
                            int targetInstanceCount = Math.Min(this.burstLimit, site.Endpoints.Count + 1);
                            InstanceEndpoint newInstance = await this.ScaleOutAsync(
                                site,
                                targetInstanceCount,
                                bestInstance);
                            return newInstance.IPAddress;
                        }
                    }
                }
            }

            // Pick one request at a time to use for latency tracking
            uint now = GetCurrentTickCount();
            if (Interlocked.CompareExchange(ref bestInstance.HealthTrackingRequestId, requestId, null) == null)
            {
                bestInstance.HealthTrackingRequestStartTime = now;
            }

            if (bestInstance.PendingRequestCount <= 1)
            {
                // This is an idle worker (the current request is the pending one).
                return bestInstance.IPAddress;
            }

            if (bestInstance.IsBusy)
            {
                using (var result = await site.ScaleLock.LockAsync(MaxLockTime))
                {
                    if (result.TimedOut)
                    {
                        // Scale operations are unhealthy
                        Debug.WriteLine("Timed-out waiting to start a scale operation on busy instance.");
                        AntaresEventProvider.EventWriteLBHttpDispatchSiteWarningMessage(site.Name, "DispatchRequest", string.Format("Timed-out ({0}ms) waiting to start a scale operation on a busy instance {1}.", MaxLockTime, bestInstance.IPAddress));
                        return bestInstance.IPAddress;
                    }

                    if (!bestInstance.IsBusy)
                    {
                        // The instance became healthy while we were waiting.
                        return bestInstance.IPAddress;
                    }

                    // Serialize scale-out requests to avoid overloading our infrastructure. Each serialized scale
                    // request will request one more instance from the previous request which can result in rapid-scale out.
                    bestInstance = await this.ScaleOutAsync(site, site.Endpoints.Count + 1, bestInstance);
                    if (bestInstance.IsBusy)
                    {
                        // Scale-out failed
                        Debug.WriteLine("Best instance is still busy after a scale operation.");
                        AntaresEventProvider.EventWriteLBHttpDispatchSiteWarningMessage(site.Name, "DispatchRequest", string.Format("Best instance {0} is still busy after a scale operation.", bestInstance.IPAddress));
                    }

                    return bestInstance.IPAddress;
                }
            }

            bool isQuestionable =
                bestInstance.PendingRequestCount > QuestionablePendingRequestCount ||
                bestInstance.HealthTrackingRequestStartTime < now - QuestionableRequestLatency;

            if (isQuestionable &&
                bestInstance.NextAllowedPingTime <= now &&
                Interlocked.CompareExchange(ref bestInstance.PingLock, 1, 0) == 0)
            {
                bool isHealthy;
                try
                {
                    isHealthy = await this.PingAsync(siteName, bestInstance, defaultHostName);
                }
                finally
                {
                    bestInstance.NextAllowedPingTime = now + PingInterval;
                    bestInstance.PingLock = 0;
                }

                if (!isHealthy)
                {
                    site.Endpoints.SetIsBusy(bestInstance);

                    // Serialize scale-out requests to avoid overloading our infrastructure. Each serialized scale
                    // request will request one more instance from the previous request which can result in rapid-scale out.
                    using (var result = await site.ScaleLock.LockAsync(MaxLockTime))
                    {
                        if (result.TimedOut)
                        {
                            // Scale operations are unhealthy
                            Debug.WriteLine("Timed-out waiting to start a scale operation after unhealthy ping.");
                            AntaresEventProvider.EventWriteLBHttpDispatchSiteWarningMessage(site.Name, "DispatchRequest", string.Format("Timed-out ({0}ms) waiting to start a scale operation after unhealthy ping to {1}.", MaxLockTime, bestInstance.IPAddress));
                            return bestInstance.IPAddress;
                        }

                        bestInstance = await this.ScaleOutAsync(site, site.Endpoints.Count + 1, bestInstance);
                        if (bestInstance.IsBusy)
                        {
                            // Scale-out failed
                            Debug.WriteLine("Best worker is still busy after a ping-initiated scale.");
                            AntaresEventProvider.EventWriteLBHttpDispatchSiteWarningMessage(site.Name, "DispatchRequest", string.Format("Best worker {0} is still busy after a ping-initiated scale.", bestInstance.IPAddress));
                        }

                        return bestInstance.IPAddress;
                    }
                }
            }

            return bestInstance.IPAddress;
        }

        public void OnRequestCompleted(
            string requestId,
            string siteName,
            int statusCode,
            string statusPhrase,
            string ipAddress)
        {
            SiteMetadata site;
            if (!this.knownSites.TryGetValue(siteName, out site))
            {
                throw new ArgumentException(string.Format("Site '{0}' does not exist in the list of known sites.", siteName));
            }

            InstanceEndpoint instance;
            if (!site.Endpoints.TryGetValue(ipAddress, out instance))
            {
                Debug.WriteLine("OnRequestCompleted: Worker '{0}' was not found.", (object)ipAddress);
                AntaresEventProvider.EventWriteLBHttpDispatchEndpointInfoMessage(site.Name, ipAddress, "OnRequestCompleted", "Worker not found");
                return;
            }

            site.Endpoints.OnRequestCompleted(instance);

            // If this is the request we're using as a health sample, clear it so another request can be selected.
            if (instance.HealthTrackingRequestId == requestId)
            {
                instance.HealthTrackingRequestId = null;
            }

            // If the request is rejected because the instance is too busy, mark the endpoint as busy for the next N seconds.
            // This flag will be cleared the next time a request arrives after the busy status expires.
            if (statusCode == 429 && statusPhrase == "Instance Too Busy")
            {
                site.Endpoints.SetIsBusy(instance);
            }
        }

        protected abstract Task<string[]> OnScaleOutAsync(string siteName, int targetInstanceCount);

        protected abstract Task<HttpStatusCode?> OnPingAsync(string siteName, string ipAddress, string defaultHostName);

        // Intended for unit testing
        protected IReadOnlyList<string> GetWorkerList(string siteName)
        {
            SiteMetadata site;
            if (this.knownSites.TryGetValue(siteName, out site))
            {
                return site.Endpoints.GetIPAddresses();
            }

            return new string[0];
        }

        // Intended for unit testing
        protected bool GetBurstModeStatus(string siteName)
        {
            SiteMetadata site;
            return this.knownSites.TryGetValue(siteName, out site) && site.IsBurstMode;
        }

        private static uint GetCurrentTickCount()
        {
            // Gets the number of milliseconds elapsed since the system started. The int value overflows at 24.9 days
            // and wraps around to -int.MaxValue. Converting to uint gives us almost 50 days of valid use, and Azure
            // VMs should never be up that long (due to monthly OS patching).
            return (uint)Environment.TickCount;
        }

        private int UpdateWorkerList(SiteMetadata site, string[] currentWorkerSet)
        {
            uint now = GetCurrentTickCount();

            // Union the cached list of known workers with the list provided by the FE. The list from the FE
            // may be stale, so we have to accept new workers generously and carefully age out stale workers.
            foreach (string ipAddress in currentWorkerSet)
            {
                InstanceEndpoint instance = site.Endpoints.GetOrAdd(ipAddress);
                instance.LastRefreshTimestamp = now;

                // Clear the busy status for workers whose busy status is ready to expire.
                if (instance.IsBusy && instance.IsBusyUntil < now)
                {
                    site.Endpoints.ClearBusyStatus(instance);
                }

                // Periodically trace the health statistics of the workers
                if (instance.NextMetricsTraceTime == 0)
                {
                    instance.NextMetricsTraceTime = now + WorkerMetricsTraceInterval;
                }
                else if (now > instance.NextMetricsTraceTime && Monitor.TryEnter(instance))
                {
                    try
                    {
                        Debug.WriteLine("UpdateWorkerList: Worker metrics for site {0}: {1}", site.Name, instance.ToString());
                        AntaresEventProvider.EventWriteLBHttpDispatchEndpointMetrics(site.Name, instance.IPAddress, instance.PendingRequestCount, instance.IsBusy, instance.Weight);
                        instance.NextMetricsTraceTime = now + WorkerMetricsTraceInterval;
                    }
                    finally
                    {
                        Monitor.Exit(instance);
                    }
                }
            }

            // This is a performance-sensitive code path so we throttle the age-out logic to avoid using excess CPU.
            if (now >= site.NextWorkerExpirationTick && Monitor.TryEnter(site))
            {
                try
                {
                    site.Endpoints.RemoveStaleEntries(site.NextWorkerExpirationTick);

                    // Wait M seconds before doing another worker expiration check.
                    site.NextWorkerExpirationTick = now + WorkerExpirationCheckInterval;
                }
                finally
                {
                    Monitor.Exit(site);
                }
            }

            site.IsBurstMode = site.Endpoints.Count < this.burstLimit;
            return site.Endpoints.Count;
        }

        // Caller must be holding an async lock
        private async Task<InstanceEndpoint> ScaleOutAsync(
            SiteMetadata site,
            int targetInstanceCount,
            InstanceEndpoint previousEndpoint)
        {
            Debug.WriteLine("Attempting to scale out to " + targetInstanceCount);
            AntaresEventProvider.EventWriteLBHttpDispatchSiteInfoMessage(site.Name, "ScaleOut", string.Format("Attempting to scale out to {0} instances.", targetInstanceCount));
            string[] ipAddresses = await this.OnScaleOutAsync(site.Name, targetInstanceCount);
            if (ipAddresses != null)
            {
                this.UpdateWorkerList(site, ipAddresses);
            }

            // It is expected in most cases that this will return the newly added worker (if any).
            return site.Endpoints.ReserveBestInstance(previousEndpoint);
        }

        private async Task<bool> PingAsync(string siteName, InstanceEndpoint instance, string defaultHostName)
        {
            // A null status code is interpreted to be equivalent to a timeout or an error sending the ping.
            // All other status codes are assumed to mean that the endpoint is unhealthy.
            HttpStatusCode? statusCode = await this.OnPingAsync(siteName, instance.IPAddress, defaultHostName);
            return statusCode != null && statusCode < HttpStatusCode.BadRequest;
        }
    }
}
