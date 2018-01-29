//------------------------------------------------------------------------------
// <copyright file="InstanceEndpoint.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Generic;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    class InstanceEndpoint : IComparable<InstanceEndpoint>
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
        public uint NextMetricsTraceTime { get; set; }

        internal LinkedListNode<InstanceEndpoint> Node { get; set; }

        /// <summary>
        /// Gets a value used to measure the load on the current instance.
        /// </summary>
        internal int Weight
        {
            // TODO: Look at other factors, like latency or success rates
            get { return this.PendingRequestCount + (this.IsBusy ? 1000 : 0); }
        }

        public int CompareTo(InstanceEndpoint other)
        {
            return this.Weight.CompareTo(other.Weight);
        }

        public override string ToString()
        {
            return string.Format(
                "{0}: PendingRequestCount = {1}, IsBusy = {2}, Weight = {3}",
                this.IPAddress,
                this.PendingRequestCount,
                this.IsBusy,
                this.Weight);
        }
    }
}
