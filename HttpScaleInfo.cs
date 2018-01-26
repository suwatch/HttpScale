//------------------------------------------------------------------------------
// <copyright file="HttpScaleInfo.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public class HttpScaleInfo
    {
        public HostingInfo HostingInfo { get; set; }

        public string RequestId { get; set; }

        public string ForwardFrontEnd { get; set; }
    }
}