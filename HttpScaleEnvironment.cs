//------------------------------------------------------------------------------
// <copyright file="HttpScaleEnvironment.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public class HttpScaleEnvironment
    {
        private readonly static TimeSpan _configCacheValidity = TimeSpan.FromMinutes(10);
        private DateTime _configCacheExpiry = DateTime.MinValue;

        private readonly static TimeSpan _frontEndsCacheValidity = TimeSpan.FromMinutes(1);
        private DateTime _frontEndsCacheExpiry = DateTime.MinValue;

        private int _maxPingLatencySeconds = 1;
        private int _maxPingConcurrent = 2;

        public static Lazy<HttpScaleEnvironment> _instance = new Lazy<HttpScaleEnvironment>(() =>
        {
            var instance = new HttpScaleEnvironment();
            instance.Initialize();
            return instance;
        });

        /// <summary>
        /// The current frontend.
        /// </summary>
        public string CurrentFrontEnd
        {
            get;
            set;
        }

        /// <summary>
        /// List of alive frontends in deterministic order.
        /// </summary>
        public IList<string> FrontEnds
        {
            get;
            set;
        }

        public static HttpScaleEnvironment Instance
        {
            get { return _instance.Value; }
        }

        /// <summary>
        /// This is milliseconds since computer starts.  Overflow at 24.9 * 2 days which is unlikely for Azure VM.
        /// </summary>
        public static uint TickCount
        {
            get { return (uint)Environment.TickCount; }
        }

        /// <summary>
        /// This determines how many FrontEnds should function http requests affinitize to.
        /// If less than or equals to 0, the feature is disabled and http scale is handled via ScaleController (legacy)
        /// If 1 or 2, site http requests will be deterministically forward to 1 or 2 frontends respectively and scale decision is done by those only.
        /// If greater than 2, site http requests will not be forward and scale decision is done from each FrontEnds independently.
        /// </summary>
        public int DynamicComputeHttpScaleMaxFrontEndsPerSite
        {
            get;
            set;
        }

        public int DynamicComputeHttpScaleBurstLimit
        {
            get;
            set;
        }

        public int DynamicComputeHttpScaleMaxPingLatencySeconds
        {
            get { return _maxPingLatencySeconds; }
            set { _maxPingLatencySeconds = value; }
        }

        public int DynamicComputeHttpScaleMaxPingConcurrent
        {
            get { return _maxPingConcurrent; }
            set { _maxPingConcurrent = value; }
        }

        public void Initialize()
        {
            CurrentFrontEnd = SiteManager.GetCurrentRoleInstanceName();
            if (string.IsNullOrEmpty(CurrentFrontEnd))
            {
                throw new NotSupportedException("No Azure role environment was detected.");
            }

            RefreshCacheIfExpired();
            RefreshFrontEndsCacheIfExpired();

            ThreadPool.QueueUserWorkItem(async delegate
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromSeconds(30));

                    try
                    {
                        RefreshCacheIfExpired();
                        RefreshFrontEndsCacheIfExpired();
                    }
                    catch (Exception)
                    {
                        // TODO, suwatch: trace
                    }
                }
            });
        }

        private void RefreshCacheIfExpired()
        {
            if (DateTime.UtcNow > _configCacheExpiry)
            {
                try
                {
                    using (var siteManager = new SiteManager { CacheEnabled = false })
                    {
                        DynamicComputeHttpScaleMaxFrontEndsPerSite = siteManager.HostingConfiguration.DynamicComputeHttpScaleMaxFrontEndsPerSite;
                        DynamicComputeHttpScaleBurstLimit = siteManager.HostingConfiguration.DynamicComputeHttpScaleBurstLimit;
                    }
                }
                catch (Exception)
                {
                    // if failed, fallback to previous value
                    // TODO, suwatch: trace
                }

                _configCacheExpiry = DateTime.UtcNow.Add(_configCacheValidity);
            }
        }

        private void RefreshFrontEndsCacheIfExpired()
        {
            if (DateTime.UtcNow > _frontEndsCacheExpiry)
            {
                try
                {
                    using (var siteManager = new SiteManager { CacheEnabled = false })
                    {
                        var frontEnds = siteManager.GetAliveFrontEnds(aliveIntervalInMinutes: 1)
                                                   .OrderBy(f => f.FrontEndId)
                                                   .Select(f => f.Name).ToList();

                        if (FrontEnds == null || string.Join(",", FrontEnds) != string.Join(",", frontEnds))
                        {
                            // TODO, suwatch: set and trace that the FrontEnds list has changed
                            FrontEnds = frontEnds;
                        }
                    }
                }
                catch (Exception)
                {
                    // if failed, fallback to previous value
                    // TODO, suwatch: trace
                }

                _frontEndsCacheExpiry = DateTime.UtcNow.Add(_frontEndsCacheValidity);
            }
        }
    }
}