//------------------------------------------------------------------------------
// <copyright file="SortedEndpointCollection.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.Web.Hosting.Tracing;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    class SortedEndpointCollection
    {
        private readonly LinkedList<InstanceEndpoint> innerList;
        private readonly SiteMetadata site;

        public SortedEndpointCollection(SiteMetadata site)
        {
            this.site = site;
            this.innerList = new LinkedList<InstanceEndpoint>();
        }

        public int Count
        {
            get { return this.innerList.Count; }
        }

        public bool TryGetValue(string ipAddress, out InstanceEndpoint result)
        {
            lock (this.innerList)
            {
                return this.TryGetValueInternal(ipAddress, out result);
            }
        }

        public InstanceEndpoint GetOrAdd(string ipAddress)
        {
            InstanceEndpoint newInstance;
            lock (this.innerList)
            {
                InstanceEndpoint existingInstance;
                if (this.TryGetValueInternal(ipAddress, out existingInstance))
                {
                    return existingInstance;
                }

                newInstance = new InstanceEndpoint(ipAddress);
                newInstance.Node = this.innerList.AddFirst(newInstance);
            }

            Debug.WriteLine("UpdateWorkerList: Added worker {0}.", (object)ipAddress);
            AntaresEventProvider.EventWriteLBHttpDispatchEndpointInfoMessage(this.site.Name, ipAddress, "UpdateWorkerList", "Added worker");
            return newInstance;
        }

        // Intended for unit testing
        public IReadOnlyList<string> GetIPAddresses()
        {
            lock (this.innerList)
            {
                return this.innerList.Select(e => e.IPAddress).ToList();
            }
        }

        // Caller must be holding the lock
        private bool TryGetValueInternal(string ipAddress, out InstanceEndpoint result)
        {
            foreach (var instance in this.innerList)
            {
                if (instance.IPAddress == ipAddress)
                {
                    result = instance;
                    return true;
                }
            }

            result = null;
            return false;
        }

        public InstanceEndpoint ReserveBestInstance(InstanceEndpoint previousEndpoint)
        {
            lock (this.innerList)
            {
                // The first instance in the list is always the best.
                LinkedListNode<InstanceEndpoint> reserved = this.innerList.First;
                reserved.Value.PendingRequestCount++;

                if (this.innerList.Count > 1)
                {
                    // The list is always sorted and we assume that in the common case incrementing
                    // the pending request count will naturally move the item to the back of the 
                    // list. As a result, this operation is expected to be O(1) in most cases.
                    this.UpdatePositionFromBack(reserved);

                    if (previousEndpoint != null)
                    {
                        previousEndpoint.PendingRequestCount--;

                        // This was previously the best, but was downgraded temporarily.
                        // try to find it from the back. It will likely need to be moved 
                        // toward the front.
                        this.UpdatePositionFromFront(previousEndpoint.Node);
                    }
                }

                return reserved.Value;
            }
        }

        public void OnRequestCompleted(InstanceEndpoint instance)
        {
            lock (this.innerList)
            {
                instance.PendingRequestCount--;

                if (this.innerList.Count > 1)
                {
                    // Move the instance towards the front of the list now that
                    // its availability has increased.
                    this.UpdatePositionFromFront(instance.Node);
                }
            }
        }

        public int RemoveStaleEntries(uint expirationTime)
        {
            int removedCount = 0;

            lock (this.innerList)
            {
                LinkedListNode<InstanceEndpoint> current = this.innerList.First;
                while (current != null)
                {
                    LinkedListNode<InstanceEndpoint> next = current.Next;

                    InstanceEndpoint instance = current.Value;
                    if (instance.LastRefreshTimestamp < expirationTime)
                    {
                        this.innerList.Remove(current);
                        removedCount++;

                        Debug.WriteLine("UpdateWorkerList: Removed worker {0} from the list.", (object)instance.IPAddress);
                        AntaresEventProvider.EventWriteLBHttpDispatchEndpointInfoMessage(site.Name, instance.IPAddress, "UpdateWorkerList", "Removing worker from routing list");
                    }

                    current = next;
                }
            }

            return removedCount;
        }

        internal void SetIsBusy(InstanceEndpoint instance)
        {
            lock (this.innerList)
            {
                uint duration = SiteRequestDispatcher.BusyStatusDuration;
                instance.IsBusyUntil = HttpScaleEnvironment.TickCount + duration;
                instance.IsBusy = true;

                Debug.WriteLine("Set instance {0} as busy for the next {1}ms. New weight: {2}", instance.IPAddress, duration, instance.Weight);
                AntaresEventProvider.EventWriteLBHttpDispatchEndpointInfoMessage(site.Name, instance.IPAddress, "SetIsBusy", string.Format("Set instance busy for {0}ms. New weight: {1}", duration, instance.Weight));

                // Busy adds a large amount of weight to an instance, moving it towards the back.
                this.UpdatePositionFromBack(instance.Node);
            }
        }

        internal void ClearBusyStatus(InstanceEndpoint instance)
        {
            lock (this.innerList)
            {
                instance.IsBusy = false;

                // Clearing the busy flag is going to give it a big priority boost,
                // though it's not clear where it will end up. Since most instances will
                // have similar status, it should be safe to assume clearing a busy flag
                // will move it toward the front of the priority list.
                this.UpdatePositionFromFront(instance.Node);

                Debug.WriteLine("Removed busy status from {0}. New weight: {1}", instance.IPAddress, instance.Weight);
                AntaresEventProvider.EventWriteLBHttpDispatchEndpointInfoMessage(site.Name, instance.IPAddress, "ClearBusyStatus", "Removing busy status. New weight: " + instance.Weight);
            }
        }

        // Caller should be holding a lock on the list.
        void UpdatePositionFromBack(LinkedListNode<InstanceEndpoint> node)
        {
            // Remove it from the list so we can re-add it.
            this.innerList.Remove(node);

            LinkedListNode<InstanceEndpoint> current = this.innerList.Last;
            while (current != null && node.Value.CompareTo(current.Value) < 0)
            {
                current = current.Previous;
            }

            if (current != null)
            {
                this.innerList.AddAfter(current, node);
            }
            else
            {
                // Worst-case scenario
                this.innerList.AddFirst(node);
            }
        }

        // Caller should be holding a lock on the list.
        void UpdatePositionFromFront(LinkedListNode<InstanceEndpoint> node)
        {
            this.innerList.Remove(node);

            LinkedListNode<InstanceEndpoint> current = this.innerList.First;
            while (current != null && node.Value.CompareTo(current.Value) > 0)
            {
                current = current.Next;
            }

            if (current != null)
            {
                this.innerList.AddBefore(current, node);
            }
            else
            {
                // Worst-case scenario
                this.innerList.AddLast(node);
            }
        }
    }
}
