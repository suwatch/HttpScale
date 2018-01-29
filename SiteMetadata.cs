//------------------------------------------------------------------------------
// <copyright file="SiteMetadata.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    class SiteMetadata
    {
        internal readonly AsyncLock ScaleLock = new AsyncLock();

        public SiteMetadata(string name)
        {
            this.Name = name;
            this.Endpoints = new SortedEndpointCollection(this);
        }

        public string Name { get; private set; }

        public bool IsBurstMode { get; set; }

        public SortedEndpointCollection Endpoints { get; private set; }

        public uint NextWorkerExpirationTick { get; set; }

        public uint NextAllowedScaleOut { get; set; }
    }
}
