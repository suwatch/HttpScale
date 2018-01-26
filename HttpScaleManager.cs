//------------------------------------------------------------------------------
// <copyright file="HttpScaleManager.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Web.Hosting.DataLayer;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public class HttpScaleManager
    {
        public readonly static HttpScaleManager Instance = new HttpScaleManager();

        private readonly ConcurrentDictionary<string, Lazy<HttpScaleDispatcher>> _dispatchers;
        private DateTime _nextCleanUpTime;

        public HttpScaleManager()
        {
            _dispatchers = new ConcurrentDictionary<string, Lazy<HttpScaleDispatcher>>();
            _nextCleanUpTime = DateTime.UtcNow.AddHours(1);
        }

        public async Task<string> OnBeginRequest(string requestId, string runtimeSiteName, string defaultHostName, IList<string> workers)
        {
            var dispatcher = _dispatchers.GetOrAdd(runtimeSiteName, n => new Lazy<HttpScaleDispatcher>(() => new HttpScaleDispatcher(n, defaultHostName)));
            return await dispatcher.Value.DispatchRequestAsync(workers, requestId);
        }

        public void OnEndRequest(string requestId, string runtimeSiteName, HttpStatusCode statusCode, string statusReason)
        {
            Lazy<HttpScaleDispatcher> dispatcher;
            if (_dispatchers.TryGetValue(runtimeSiteName, out dispatcher))
            {
                dispatcher.Value.OnRequestCompleted(requestId, statusCode, statusReason);
            }

            if (_nextCleanUpTime < DateTime.UtcNow)
            {
                _nextCleanUpTime = DateTime.UtcNow.AddHours(1);
                CleanupCache();
            }
        }

        private void CleanupCache()
        {
            // no requests in the past hours
            var staleTime = DateTime.UtcNow.Subtract(TimeSpan.FromHours(1));
            foreach (var stale in _dispatchers.Where(d => d.Value.Value.IsStale(staleTime)).ToList())
            {
                Lazy<HttpScaleDispatcher> unused;
                _dispatchers.TryRemove(stale.Key, out unused);
            }
        }

        private static async Task<IList<string>> AddWorkerAsync(string runtimeSiteName, int targetWorkerCount)
        {
            var client = GetDataServiceClient();
            EnsureWorkersForSiteResult result;
            try
            {
                result = await client.EnsureWorkersForSite(runtimeSiteName, targetWorkerCount);
            }
            catch (WebHostingObjectNotFoundException)
            {
                // TODO, suwatch: tracing
                return null;
            }

            if (result.NeedMoreCapacity)
            {
                // TODO, suwatch: check id regional scalable and try notify full
                return null;
            }

            return result.AssignedWorkerNames != null ? result.AssignedWorkerNames.ToList() : null;
        }

        private static Task<bool> RemoveWorkerAsync(string runtimeSiteName, string workerName)
        {
            return Task.FromResult(true);
        }

        private static async Task<HttpStatusCode> PingWorkerAsync(string runtimeSiteName, string workerName, string defaultHostName)
        {
            return await ExecuteWithTimeout(async (timeout, cancellationToken) =>
            {
                // TODO, suwatch: tracing
                var expirationTime = DateTimeOffset.UtcNow.AddMinutes(30);
                var token = SecurityManager.Default.CreateWorkerAuthorizationToken(workerName, runtimeSiteName, expirationTime).SerializeToken();
                var requestUri = string.Format("http://{0}", workerName);
                using (var client = new HttpClient())
                {
                    client.Timeout = timeout;
                    var request = new HttpRequestMessage(HttpMethod.Get, requestUri);
                    request.Headers.Host = runtimeSiteName;
                    request.Headers.Add("User-Agent", "HttpScaleManager/1.0.0");
                    request.Headers.Add("Disguised-Host", defaultHostName);

                    using (var response = await client.SendAsync(request))
                    {
                        return response.StatusCode;
                    }
                }
            }, TimeSpan.FromSeconds(HttpScaleEnvironment.Instance.DynamicComputeHttpScaleMaxPingLatencySeconds), HttpStatusCode.RequestTimeout);
        }

        private static async Task<T> ExecuteWithTimeout<T>(Func<TimeSpan, CancellationToken, Task<T>> func, TimeSpan timeout, T error)
        {
            try
            {
                var cancel = new CancellationTokenSource();
                var delay = Task.Delay(timeout, cancel.Token);
                var task = func(timeout, cancel.Token);
                var any = await Task.WhenAny(delay, task);

                cancel.Cancel();
                if (any == task)
                {
                    return task.Result;
                }
            }
            catch (Exception)
            {
            }

            return error;
        }

        private static DataServiceHttpClient GetDataServiceClient()
        {
            DataServiceClientConfiguration dataServiceConfig = SiteManager.GetDataServiceClientConfiguration();
            return new DataServiceHttpClient(dataServiceConfig);
        }

        class HttpScaleDispatcher : SiteRequestDispatcher
        {
            private readonly string _runtimeSiteName;
            private readonly string _defaultHostName;
            private DateTime _recentRequestTime;

            // TODO, suwatch: burst value should flow at runtime
            public HttpScaleDispatcher(string runtimeSiteName, string defaultHostName) 
                : base(runtimeSiteName, HttpScaleEnvironment.Instance)
            {
                _runtimeSiteName = runtimeSiteName;
                _defaultHostName = defaultHostName;
                _recentRequestTime = DateTime.UtcNow;
            }

            public bool IsStale(DateTime staleTime)
            {
                return staleTime > _recentRequestTime;
            }

            public override Task<string> DispatchRequestAsync(IList<string> workerList, string requestId)
            {
                _recentRequestTime = DateTime.UtcNow;
                return base.DispatchRequestAsync(workerList, requestId);
            }

            protected override Task<IList<string>> OnScaleOutAsync(int targetWorkerCount)
            {
                return AddWorkerAsync(_runtimeSiteName, targetWorkerCount);
            }

            protected virtual Task<bool> OnRemoveWorkerAsync(string workerName)
            {
                return RemoveWorkerAsync(_runtimeSiteName, workerName);
            }

            protected override Task<HttpStatusCode> OnPingAsync(string workerName)
            {
                return PingWorkerAsync(_runtimeSiteName, workerName, _defaultHostName);
            }
        }
    }
}