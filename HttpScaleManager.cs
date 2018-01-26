//------------------------------------------------------------------------------
// <copyright file="HttpScaleManager.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Web.Hosting.DataLayer;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public class HttpScaleManager : SiteRequestDispatcher
    {
        public readonly static HttpScaleManager Instance = new HttpScaleManager();

        private readonly static HttpClient PingClient = new HttpClient();

        public HttpScaleManager()
            : base(burstLimit: 4)
        {
        }

        protected override async Task<string[]> OnScaleOutAsync(string runtimeSiteName, int targetWorkerCount)
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

            return result.AssignedWorkerNames != null ? result.AssignedWorkerNames.ToArray() : null;
        }

        protected override async Task<HttpStatusCode?> OnPingAsync(string runtimeSiteName, string ipAddress, string defaultHostName)
        {
            HttpStatusCode? result;
            WebExceptionStatus status = WebExceptionStatus.Success;
            Stopwatch stopwatch = new Stopwatch();

            // TODO, suwatch: tracing
            // A token is only needed to wake up the worker process. If it's already awake, then a token is not needed.
            // Possible to optimize so that we don't use the token in the happy case.
            DateTimeOffset expirationTime = DateTimeOffset.UtcNow.AddMinutes(30);
            string token = SecurityManager.Default.CreateWorkerAuthorizationToken(ipAddress, runtimeSiteName, expirationTime).SerializeToken();
            string requestUri = string.Format("http://{0}", ipAddress);
            using (var request = new HttpRequestMessage(HttpMethod.Get, requestUri))
            {
                request.Headers.Host = runtimeSiteName;
                request.Headers.Add("User-Agent", "HttpScaleManager/1.0.0");
                request.Headers.Add("MWH-SecurityToken", token);
                request.Headers.Add("Disguised-Host", defaultHostName);

                TimeSpan timeout = TimeSpan.FromSeconds(HttpScaleEnvironment.Instance.DynamicComputeHttpScaleMaxPingLatencySeconds);
                stopwatch.Start();

                try
                {
                    using (var timeoutTokenSource = new CancellationTokenSource(timeout))
                    using (var response = await PingClient.SendAsync(request, timeoutTokenSource.Token))
                    {
                        result = response.StatusCode;
                    }
                }
                catch (OperationCanceledException)
                {
                    result = HttpStatusCode.RequestTimeout;
                }
                catch (WebException e)
                {
                    status = e.Status;
                    result = null;
                }
            }

            Debug.WriteLine("Pinged site '{0}' at {1}. Status: {2}, Result: {3}, Latency: {4}ms", runtimeSiteName, ipAddress, status, result, stopwatch.ElapsedMilliseconds);
            return result;
        }

        private static Task<bool> RemoveWorkerAsync(string runtimeSiteName, string workerName)
        {
            return Task.FromResult(true);
        }

        private static DataServiceHttpClient GetDataServiceClient()
        {
            DataServiceClientConfiguration dataServiceConfig = SiteManager.GetDataServiceClientConfiguration();
            return new DataServiceHttpClient(dataServiceConfig);
        }
    }
}