//------------------------------------------------------------------------------
// <copyright file="HttpScaleHandler.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Web.Hosting.Common;
using Microsoft.Web.Hosting.Security;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public class HttpScaleHandler
    {
        public static Lazy<HttpScaleHandler> _instance = new Lazy<HttpScaleHandler>(() =>
        {
            return new HttpScaleHandler(HttpScaleManager.Instance, HttpScaleEnvironment.Instance);
        });

        private static long _requestId = 0;

        private readonly HttpScaleManager _manager;
        private readonly HttpScaleEnvironment _environment;
        private readonly CacheManager<string> _createdTokenCaches;
        private readonly CacheManager<WorkerAuthorizationToken> _validatedTokenCaches;

        public HttpScaleHandler(HttpScaleManager manager, HttpScaleEnvironment configuration)
        {
            _manager = manager;
            _environment = configuration;
            _createdTokenCaches = new CacheManager<string>(0, 1024, TimeSpan.FromMinutes(25));
            _validatedTokenCaches = new CacheManager<WorkerAuthorizationToken>(0, 1024, TimeSpan.FromMinutes(15));
        }

        public static HttpScaleHandler Instance
        {
            get { return _instance.Value; }
        }

        public async Task<HttpScaleInfo> OnBeginRequest(HostingInfo hostingInfo, string workerToken)
        {
            if (hostingInfo == null)
            {
                return null;
            }

            if (_environment.DynamicComputeHttpScaleMaxFrontEndsPerSite <= 0)
            {
                return null;
            }

            if (!ShouldScaleHttpFunction(hostingInfo))
            {
                return null;
            }

            string[] workers;
            if (!TryGetWorkers(hostingInfo, out workers))
            {
                return null;
            }

            WorkerAuthorizationToken token = null;
            if (!string.IsNullOrEmpty(workerToken) && !TryValidateWorkerToken(hostingInfo, workerToken, out token))
            {
                return null;
            }

            if (token == null)
            {
                var forwardFrontEnd = GetForwardFrontEnd(hostingInfo.SiteName, _environment);

                // do not forward, if forward FE is current
                if (forwardFrontEnd == _environment.CurrentFrontEnd)
                {
                    // worker selection
                    var requestId = NextHttpScaleRequestId(hostingInfo.SiteName);
                    var selected = await _manager.DispatchRequestAsync(requestId, hostingInfo.SiteName, hostingInfo.DefaultHostName, workers);

                    var info = new HttpScaleInfo();
                    info.RequestId = string.Format("{0}/{1}", requestId, selected);
                    info.HostingInfo = new HostingInfo(hostingInfo);
                    info.HostingInfo.ServerListOrError = selected;

                    // for newly added worker, create WorkerAuthorizationToken
                    if (!workers.Any(w => w == selected))
                    {
                        info.HostingInfo.Token = CreateWorkerToken(selected, hostingInfo.SiteName);
                    }

                    return info;
                }
                else
                {
                    var info = new HttpScaleInfo();
                    // TODO, suwatch: sampling tracking FE organic health
                    // info.RequestId = forwardFrontEnd;
                    info.ForwardFrontEnd = forwardFrontEnd;
                    info.HostingInfo = new HostingInfo(hostingInfo);
                    info.HostingInfo.ServerListOrError = forwardFrontEnd;
                    return info;
                }
            }
            else
            {
                // this is forwarded requests
                var requestId = NextHttpScaleRequestId(hostingInfo.SiteName);
                var selected = await _manager.DispatchRequestAsync(requestId, hostingInfo.SiteName, hostingInfo.DefaultHostName, workers.Length >= token.Workers.Length ? workers : token.Workers);

                var info = new HttpScaleInfo();
                info.RequestId = string.Format("{0}/{1}", requestId, selected);
                info.HostingInfo = new HostingInfo(hostingInfo);
                info.HostingInfo.ServerListOrError = selected;

                // for newly added worker, create WorkerAuthorizationToken
                if (!workers.Any(w => w == selected))
                {
                    if (token.Workers.Any(w => w == selected))
                    {
                        info.HostingInfo.Token = workerToken;
                    }
                    else
                    {
                        info.HostingInfo.Token = CreateWorkerToken(selected, hostingInfo.SiteName);
                    }
                }

                return info;
            }
        }

        public void OnEndRequest(string requestId, HttpStatusCode statusCode, string statusReason)
        {
            if (requestId.Length < 20)
            {
                // TODO, suwatch: sampling tracking FE organic health
            }
            else
            {
                string runtimeSiteName;
                string originalRequestId;
                string workerName;
                ParseCompletedRequestId(requestId, out runtimeSiteName, out originalRequestId, out workerName);

                _manager.OnRequestCompleted(originalRequestId, runtimeSiteName, (int)statusCode, statusReason, workerName);
            }
        }

        public virtual bool TryGetWorkers(HostingInfo hostingInfo, out string[] workers)
        {
            workers = null;
            if (string.IsNullOrEmpty(hostingInfo.ServerListOrError))
            {
                return false;
            }

            // make sure ServerListOrError is ipAddress
            if (hostingInfo.ServerListOrError[0] < '1' || hostingInfo.ServerListOrError[0] > '9')
            {
                return false;
            }

            workers = hostingInfo.ServerListOrError.Split(',');
            return true;
        }

        public virtual bool ShouldScaleHttpFunction(HostingInfo hostingInfo)
        {
            // only main site
            if (hostingInfo.HostNameType != HostNameType.Standard)
            {
                return false;
            }

            // TODO, suwatch: support client certificate
            if (hostingInfo.ClientCertEnabled)
            {
                return false;
            }

            // white list or scm site
            if (hostingInfo.NeedsPlatformAuth)
            {
                return false;
            }

            // consumption plan
            if (hostingInfo.ComputeMode != ComputeMode.Dynamic)
            {
                return false;
            }

            // http trigger exists
            return hostingInfo.HasHttpTrigger.GetValueOrDefault();
        }

        public virtual bool TryValidateWorkerToken(HostingInfo hostingInfo, string workerToken, out WorkerAuthorizationToken token)
        {
            try
            {
                WorkerAuthorizationToken result;
                if (_validatedTokenCaches.TryGetValue(workerToken, out result))
                {
                    token = result;
                    return true;
                }

                result = new WorkerAuthorizationToken(workerToken);

                string failureDetails;
                if (SecurityManager.Default.TryValidateToken(result, out failureDetails) && string.Equals(result.SiteName, hostingInfo.SiteName))
                {
                    _validatedTokenCaches.Add(workerToken, result);
                    token = result;
                    return true;
                }

                throw new InvalidOperationException(string.Format("Token validation failed.  {0}", failureDetails));
            }
            catch (Exception)
            {
                // TODO, suwatch: tracing
            }

            token = null;
            return false;
        }

        public virtual string CreateWorkerToken(string worker, string runtimeSiteName)
        {
            var key = string.Format("{0}:{1}", worker, runtimeSiteName).ToLowerInvariant();

            string token;
            if (_createdTokenCaches.TryGetValue(key, out token))
            {
                return token;
            }

            var expirationTime = DateTimeOffset.UtcNow.AddMinutes(30);
            token = SecurityManager.Default.CreateWorkerAuthorizationToken(worker, runtimeSiteName, expirationTime).SerializeToken();
            _createdTokenCaches.Add(key, token);
            return token;
        }

        public static bool ShouldHandleHttpScale(string runtimeSiteName, HttpScaleEnvironment environment)
        {
            return environment.CurrentFrontEnd == GetForwardFrontEnd(runtimeSiteName, environment);
        }

        public static string GetForwardFrontEnd(string runtimeSiteName, HttpScaleEnvironment environment)
        {
            if (environment.DynamicComputeHttpScaleMaxFrontEndsPerSite <= 0)
            {
                throw new ArgumentOutOfRangeException("DynamicComputeHttpScaleMaxFrontEndsPerSite must be greater than 0");
            }

            // each FE handles scale independently
            if (environment.DynamicComputeHttpScaleMaxFrontEndsPerSite > 2)
            {
                return environment.CurrentFrontEnd;
            }

            // this is degenerated case where no FE is alive, all FE should handle
            var frontEnds = environment.FrontEnds;
            if (frontEnds.Count == 0)
            {
                return environment.CurrentFrontEnd;
            }

            // if only one FE is alive, it should handle
            if (frontEnds.Count == 1)
            {
                return frontEnds[0];
            }

            var currentBucket = (int)(Fnv1aHashHelper.ComputeHash(runtimeSiteName) % frontEnds.Count);

            // if only forward to 1 frontend
            if (environment.DynamicComputeHttpScaleMaxFrontEndsPerSite == 1)
            {
                return frontEnds[currentBucket];
            }

            // forwarding to 2 frontends at most
            var forwardFrontEnds = new List<string>(2);
            forwardFrontEnds.Add(frontEnds[currentBucket]);
            forwardFrontEnds.Add(frontEnds[(currentBucket + 1) % frontEnds.Count]);

            if (forwardFrontEnds.Any(n => n == environment.CurrentFrontEnd))
            {
                return environment.CurrentFrontEnd;
            }

            var random = new Random();
            return forwardFrontEnds[random.Next() % forwardFrontEnds.Count];
        }

        public static string NextHttpScaleRequestId(string runtimeSiteName)
        {
            var nextId = Interlocked.Increment(ref _requestId);
            return string.Format("{0}/{1:x16}/{2}", IdnHelper.GetAscii(runtimeSiteName), (ulong)nextId, HttpScaleEnvironment.TickCount);
        }

        public static void ParseCompletedRequestId(
            string httpScaleRequestId,
            out string siteName,
            out string originalRequestId,
            out string workerName)
        {
            int firstSeparator = httpScaleRequestId.IndexOf('/');
            siteName = IdnHelper.GetUnicode(httpScaleRequestId.Substring(0, firstSeparator));

            int lastSeparator = httpScaleRequestId.LastIndexOf('/');
            originalRequestId = httpScaleRequestId.Substring(0, lastSeparator);
            workerName = httpScaleRequestId.Substring(lastSeparator + 1);
        }
    }
}