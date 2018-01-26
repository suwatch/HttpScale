//------------------------------------------------------------------------------
// <copyright file="SiteRequestDispatcher.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public abstract class SiteRequestDispatcher
    {
        // All time are in ticks, which are equivalent to milliseconds, but are cheaper to calculate
        private const uint BusyStatusDuration = 2000;
        private const uint QuestionableRequestLatency = 2000;
        private const uint WorkerExpirationDelay = 40000;
        private const uint WorkerExpirationCheckInterval = 5000;
        private const uint PingInterval = 5000;
        private const uint PingTimeout = 400;
        private const uint ScaleInterval = 1000;

        private const uint QuestionablePendingRequestCount = 50;
        private const double QuestionableInstanceRatio = 0.5;

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

            int currentInstanceCount = this.UpdateWorkerList(site, knownWorkers);

            InstanceEndpoint selectedWorker;

            bool isBurstMode = site.IsBurstMode;
            if (isBurstMode)
            {
                selectedWorker = await this.SelectBurstModeWorkerAsync(site);
            }
            else
            {
                selectedWorker = GetWorkersInPriorityOrder(site).FirstOrDefault();
                if (selectedWorker.IsBusy)
                {
                    // The best worker is busy, so we know it's time to scale.
                    InstanceEndpoint newInstance = await this.ScaleOutAsync(site, currentInstanceCount + 1);
                    selectedWorker = newInstance ?? selectedWorker;
                }

                // Pick one request at a time to use for latency tracking
                uint now = GetCurrentTickCount();
                if (Interlocked.CompareExchange(ref selectedWorker.HealthTrackingRequestId, requestId, null) == null)
                {
                    selectedWorker.HealthTrackingRequestStartTime = now;
                }

                // If it's a new worker or one that's not serving requests, we'll use it right away. Otherwise
                // we'll check to see if a ping test is needed, in which case we might decide to scale.
                if (selectedWorker.PendingRequestCount > 0 &&
                    selectedWorker.NextAllowedPingTime <= now &&
                    Interlocked.CompareExchange(ref selectedWorker.PingLock, 1, 0) == 0)
                {
                    bool isHealthy = true;
                    try
                    {
                        // Count the number of instances that are of "questionable" health. Use that number
                        // to decide whether a ping test is needed.
                        int questionableInstances = site.InstanceList.Values.Count(instance =>
                            instance.PendingRequestCount > QuestionablePendingRequestCount ||
                            instance.HealthTrackingRequestStartTime < now - QuestionableRequestLatency);

                        if (questionableInstances >= site.InstanceList.Count * QuestionableInstanceRatio)
                        {
                            isHealthy = await this.PingAsync(siteName, selectedWorker, defaultHostName);
                        }
                    }
                    finally
                    {
                        selectedWorker.NextAllowedPingTime = now + PingInterval;
                        selectedWorker.PingLock = 0;
                    }

                    if (!isHealthy)
                    {
                        selectedWorker.IsBusyUntil = now + BusyStatusDuration;
                        selectedWorker.IsBusy = true;

                        InstanceEndpoint newInstance = await this.ScaleOutAsync(site, currentInstanceCount + 1);
                        selectedWorker = newInstance ?? selectedWorker;
                    }
                }
            }

            Interlocked.Increment(ref selectedWorker.PendingRequestCount);
            return selectedWorker.IPAddress;
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

            InstanceEndpoint worker;
            if (!site.InstanceList.TryGetValue(ipAddress, out worker))
            {
                // TODO: Trace warning - the worker may have been removed before the request came back
                return;
            }

            Interlocked.Decrement(ref worker.PendingRequestCount);

            // If this is the request we're using as a health sample, clear it so another request can be selected.
            if (worker.HealthTrackingRequestId == requestId)
            {
                worker.HealthTrackingRequestId = null;
            }

            // If the request is rejected because the instance is too busy, mark the endpoint as busy for the next N seconds.
            // This flag will be cleared the next time a request arrives after the busy status expires.
            if (statusCode == 429 && statusPhrase == "Instance Too Busy")
            {
                worker.IsBusy = true;
                worker.IsBusyUntil = GetCurrentTickCount() + BusyStatusDuration;
                // TODO: Trace
            }
        }

        protected abstract Task<string[]> OnScaleOutAsync(string siteName, int targetInstanceCount);

        protected abstract Task<HttpStatusCode?> OnPingAsync(string siteName, string ipAddress, string defaultHostName);

        // Intended for unit testing
        protected IReadOnlyList<string> GetWorkerList(string siteName)
        {
            SiteMetadata site;
            if (!this.knownSites.TryGetValue(siteName, out site))
            {
                return new string[0];
            }

            lock (site)
            {
                return site.InstanceList.Keys.ToList().AsReadOnly();
            }
        }

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
                InstanceEndpoint worker = site.InstanceList.GetOrAdd(ipAddress, ip => new InstanceEndpoint(ip));
                worker.LastRefreshTimestamp = now;

                // Clear the busy status for workers whose busy status is ready to expire.
                if (worker.IsBusy && worker.IsBusyUntil < now)
                {
                    worker.IsBusy = false;
                }
            }

            // This is a performance-sensitive code path so we throttle the age-out logic to avoid using excess CPU.
            if (now >= site.NextWorkerExpirationTick && Monitor.TryEnter(site))
            {
                try
                {
                    // Expire workers that have not been seen for the last N seconds.
                    uint expirationThreshold = now - WorkerExpirationDelay;
                    foreach (InstanceEndpoint worker in site.InstanceList.Values.Where(w => w.LastRefreshTimestamp < expirationThreshold))
                    {
                        InstanceEndpoint unused;
                        site.InstanceList.TryRemove(worker.IPAddress, out unused);
                    }

                    // Wait M seconds before doing another worker expiration check.
                    site.NextWorkerExpirationTick = now + WorkerExpirationCheckInterval;
                }
                finally
                {
                    Monitor.Exit(site);
                }
            }

            site.IsBurstMode = site.InstanceList.Count < this.burstLimit;
            return site.InstanceList.Count;
        }

        private async Task<InstanceEndpoint> SelectBurstModeWorkerAsync(SiteMetadata site)
        {
            InstanceEndpoint best = null;

            // Try to get the best idle instance
            foreach (InstanceEndpoint worker in GetWorkersInPriorityOrder(site))
            {
                if (best == null)
                {
                    best = worker;
                }

                if (worker.PendingRequestCount == 0)
                {
                    return worker;
                }
            }

            // No instances are idle, scale-out and send the request to the new guy.
            // Note that there will be a cold-start penalty for this request, but it's
            // been decided that this is better than sending a request to an existing
            // but potentially CPU-pegged instance.
            int targetInstanceCount = Math.Max(this.burstLimit, site.InstanceList.Count + 1);
            InstanceEndpoint newInstance = await this.ScaleOutAsync(site, targetInstanceCount);

            // If we failed to scale out, return the best worker we have.
            return newInstance ?? best;
        }

        private static IEnumerable<InstanceEndpoint> GetWorkersInPriorityOrder(SiteMetadata site)
        {
            // TODO: Look at other factors, like latency, busy status, or status codes
            return site.InstanceList.Values.OrderBy(w => w.PendingRequestCount);
        }

        private async Task<InstanceEndpoint> ScaleOutAsync(SiteMetadata site, int targetInstanceCount)
        {
            uint now = GetCurrentTickCount();

            // Ensure that only one thread attempts to scale out at a time. Other threads will effectively no-op.
            if (site.InstanceList.Count < targetInstanceCount &&
                (site.IsBurstMode || site.NextAllowedScaleOut <= now) &&
                Interlocked.CompareExchange(ref site.ScaleLock, 1, 0) == 0)
            {
                try
                {
                    string[] ipAddresses = await this.OnScaleOutAsync(site.Name, targetInstanceCount);
                    if (ipAddresses != null)
                    {
                        this.UpdateWorkerList(site, ipAddresses);
                    }
                }
                finally
                {
                    site.NextAllowedScaleOut = now + ScaleInterval;
                    site.ScaleLock = 0;
                }
            }

            // It is expected in most cases that this will return the newly added worker (if any).
            return GetWorkersInPriorityOrder(site).FirstOrDefault();
        }

        private async Task<bool> PingAsync(string siteName, InstanceEndpoint instance, string defaultHostName)
        {
            // A null status code is interpreted to be equivalent to a timeout or an error sending the ping.
            // All other status codes are assumed to mean that the endpoint is unhealthy.
            HttpStatusCode? statusCode = await this.OnPingAsync(siteName, instance.IPAddress, defaultHostName);
            return statusCode != null && statusCode < HttpStatusCode.BadRequest;
        }

        class SiteMetadata
        {
            internal int ScaleLock;

            public SiteMetadata(string name)
            {
                this.Name = name;
                this.InstanceList = new ConcurrentDictionary<string, InstanceEndpoint>();
            }

            public string Name { get; private set; }

            public bool IsBurstMode { get; set; }

            public ConcurrentDictionary<string, InstanceEndpoint> InstanceList { get; private set; }

            public uint NextWorkerExpirationTick { get; set; }

            public uint NextAllowedScaleOut { get; set; }
        }

        class InstanceEndpoint
        {
            public int PingLock;
            public int PendingRequestCount;
            public string HealthTrackingRequestId;
            public uint HealthTrackingRequestStartTime;

            public InstanceEndpoint(string ipAddress)
            {
                this.IPAddress = ipAddress;
            }

            public string IPAddress { get; private set; }
            public bool IsBusy { get; set; }
            public uint IsBusyUntil { get; set; }
            public uint LastRefreshTimestamp { get; set; }
            public uint NextAllowedPingTime { get; set; }

            public override string ToString()
            {
                return string.Format(
                    "{0}: Requests = {1}, IsBusy = {2}",
                    this.IPAddress,
                    this.PendingRequestCount,
                    this.IsBusy);
            }
        }
    }
}
