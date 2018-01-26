//------------------------------------------------------------------------------
// <copyright file="SiteRequestDispatcher.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    public abstract class SiteRequestDispatcher
    {
        readonly string siteName;
        readonly HttpScaleEnvironment environment;

        //readonly object thisLock = new object();
        //readonly ConcurrentDictionary<string, WorkerStatus> pendingRequests;

        readonly SemaphoreSlim _scaleLock = new SemaphoreSlim(1, 1);
        ConcurrentDictionary<string, WorkerStatus> workerList;
        bool isBurstMode;

        //private uint _lastExecute;

        public SiteRequestDispatcher(string siteName, HttpScaleEnvironment environment)
        {
            this.siteName = siteName;
            this.environment = environment;

            this.workerList = new ConcurrentDictionary<string, WorkerStatus>();
            //this.pendingRequests = new ConcurrentDictionary<string, WorkerStatus>();
        }

        public ICollection<string> WorkerList
        {
            get { return this.workerList.Keys; }
        }

        public bool IsBurstMode
        {
            get { return this.isBurstMode; }
        }

        public virtual async Task<string> DispatchRequestAsync(IList<string> workerList, string requestId)
        {
            WorkerStatus selectedWorker;
            //var now = HttpScaleEnvironment.TickCount;
            //if (this.workerList.Count > 5 )
            //{
            //    if (_lastExecute > now || workerList.Count <= this.workerList.Count)
            //    {
            //        selectedWorker = this.workerList.ElementAt((int)(now % this.workerList.Count)).Value;
            //        Interlocked.Increment(ref selectedWorker.PendingRequestCount);
            //        return selectedWorker.IPAddress;
            //    }
            //}

            //_lastExecute = now + 5000;

            // TODO, cgillum: should do uninon with existing workerlist since each FE may see different list
            this.UpdateWorkerList(workerList);

            // TODO, cgillum: race condition for scaling out
            var intialCount = this.workerList.Count;

            if (this.IsBurstMode)
            {
                selectedWorker = await this.SelectBurstModeWorkerAsync();
            }
            else
            {
                selectedWorker = this.GetWorkersInPriorityOrder().First();

                // long latency
                if (this.workerList.All(w => w.Value.PendingRequestCount > 0 && w.Value.IsBusy) || this.workerList.All(w => w.Value.PendingRequestCount >= 50))
                {
                    uint latency;
                    if (selectedWorker.LastPingTick > HttpScaleEnvironment.TickCount)
                    {
                        latency = selectedWorker.LastPingLatency;
                    }
                    else
                    {
                        var start = HttpScaleEnvironment.TickCount;
                        var status = await OnPingAsync(selectedWorker.IPAddress);
                        latency = selectedWorker.LastPingLatency = HttpScaleEnvironment.TickCount - start;
                        selectedWorker.LastPingTick = HttpScaleEnvironment.TickCount + 5000;
                    }

                    //System.Diagnostics.Debug.WriteLine("Png {0} {1} {2} {3}ms", DateTime.UtcNow, selectedWorker.IPAddress, status, latency);
                    //System.Diagnostics.Debug.WriteLine(string.Empty);
                    if (latency > 100)
                    {
                        var newWorker = await ScaleOutAsync(intialCount + 1);
                        selectedWorker = newWorker ?? selectedWorker;
                    }
                }
            }

            //if (!this.pendingRequests.TryAdd(requestId, selectedWorker))
            //{
            //    throw new ArgumentException(string.Format("A request with ID '{0}' is already pending.", requestId));
            //}

            Interlocked.Increment(ref selectedWorker.PendingRequestCount);
            return selectedWorker.IPAddress;
        }

        public void OnRequestCompleted(string requestId, HttpStatusCode statusCode, string statusPhrase)
        {
            WorkerStatus worker;
            //if (!this.pendingRequests.TryRemove(requestId, out worker))
            //{
            //    // TODO: Trace warning
            //    return;
            //}

            var workerName = HttpScaleHandler.ParseWorkerName(requestId);
            //lock (thisLock)
            {
                if (!this.workerList.TryGetValue(workerName, out worker))
                {
                    // TODO: Trace warning
                    return;
                }
            }

            var tickCount = HttpScaleEnvironment.TickCount;
            worker.LastRequestCompleteTick = tickCount;
            worker.LastRequestLatencyMs = tickCount - HttpScaleHandler.ParseEnvironmentTick(requestId);
            worker.IsBusy = worker.LastRequestLatencyMs > 100;

            Interlocked.Decrement(ref worker.PendingRequestCount);

            // TODO: Check for 429 or high latency
        }

        protected abstract Task<IList<string>> OnScaleOutAsync(int targetWorkerCount);

        protected abstract Task<HttpStatusCode> OnPingAsync(string ipAddress);

        private void UpdateWorkerList(IList<string> updatedWorkerList)
        {
            if (updatedWorkerList.Count > this.workerList.Count || this.workerList.Count <= 5)
            {
                var now = HttpScaleEnvironment.TickCount;
                foreach (var workerName in updatedWorkerList)
                {
                    var workerStatus = this.workerList.GetOrAdd(workerName, ipAddress => new WorkerStatus(ipAddress));
                    workerStatus.LastRequestDispatchTick = now;
                }

                var stale = now - 30000;
                foreach (var workerName in this.workerList.Where(w => w.Value.LastRequestDispatchTick < stale).Select(w => w.Key))
                {
                    WorkerStatus unused;
                    this.workerList.TryRemove(workerName, out unused);
                }
            }

            this.isBurstMode = this.workerList.Count < this.environment.DynamicComputeHttpScaleBurstLimit;

            //System.Diagnostics.Debug.WriteLine("Upd {0} {1} workers, {2}", DateTime.UtcNow, updatedWorkerList.Count, string.Join(",", updatedWorkerList.OrderBy(n => n).ToArray()));
            //System.Diagnostics.Debug.WriteLine("New {0} {1} workers, {2}", DateTime.UtcNow, this.workerList.Count, string.Join(",", this.workerList.Select(w => w.Key).OrderBy(n => n).ToArray()));
            //System.Diagnostics.Debug.WriteLine(string.Empty);

            /*
            lock (this.thisLock)
            {
                // Replace the existing list with a new one, preserving existing stats.
                var newList = new Dictionary<string, WorkerStatus>();
                var now = DateTime.UtcNow;
                foreach (string ipAddress in updatedWorkerList)
                {
                    WorkerStatus workerStatus;
                    if (this.workerList == null || !this.workerList.TryGetValue(ipAddress, out workerStatus))
                    {
                        // newly added worker
                        workerStatus = new WorkerStatus(ipAddress);
                    }
                    else
                    {
                        workerStatus.LastRequestDispatchTick = HttpScaleEnvironment.TickCount;
                    }

                    newList.Add(ipAddress, workerStatus);
                }

                if (this.workerList != null)
                {
                    var stale = HttpScaleEnvironment.TickCount - 60000;
                    foreach (var worker in this.workerList.Where(w => w.Value.LastRequestDispatchTick > stale))
                    {
                        if (!newList.ContainsKey(worker.Key))
                        {
                            newList[worker.Key] = worker.Value;
                        }
                    }
                }

                this.isBurstMode = newList.Count < this.environment.DynamicComputeHttpScaleBurstLimit;

                //if (newList.Count >= this.configuration.DynamicComputeHttpScaleBurstLimit)
                //{
                //    // Burst mode is turned off for the remainder of the session
                //    this.isBurstMode = false;
                //}
                //else if (this.workerList.Count == 0)
                //{
                //    // Going from zero to one worker initiates burst mode
                //    this.isBurstMode = true;
                //}

                // TODO, suwatch
                //System.Diagnostics.Debug.WriteLine("Upd {0} {1}", DateTime.UtcNow, string.Join(",", updatedWorkerList.ToArray()));
                //System.Diagnostics.Debug.WriteLine("New {0} {1}", DateTime.UtcNow, string.Join(",", newList.Select(w => w.Key).ToArray()));
                //System.Diagnostics.Debug.WriteLine("Old {0} {1}", DateTime.UtcNow, string.Join(",", this.workerList.Select(w => w.Key).ToArray()));
                //System.Diagnostics.Debug.WriteLine(string.Empty);

                this.workerList = newList;
            }
            */
        }

        private async Task<WorkerStatus> SelectBurstModeWorkerAsync()
        {
            // Try to get the best idle instance
            WorkerStatus best = null;
            foreach (WorkerStatus worker in this.GetWorkersInPriorityOrder())
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
            WorkerStatus newInstance = await this.ScaleOutAsync(Math.Min(this.workerList.Count + 1, this.environment.DynamicComputeHttpScaleBurstLimit));
            return newInstance ?? best;
        }

        private IEnumerable<WorkerStatus> GetWorkersInPriorityOrder()
        {
            // TODO: Look at other factors, like latency or status codes
            // lock (this.thisLock)
            {
                //return this.workerList.Values.Where(w => !w.IsBusy).OrderBy(w => w.PendingRequestCount);
                return this.workerList.Values.OrderBy(w => w.PendingRequestCount);
            }
        }

        private async Task<WorkerStatus> ScaleOutAsync(int targetCount)
        {
            //System.Diagnostics.Debug.WriteLine("Scal {0} {1}", DateTime.UtcNow, targetCount);
            //System.Diagnostics.Debug.WriteLine(string.Empty);

            await _scaleLock.WaitAsync();
            try
            {
                if (this.workerList.Count >= targetCount)
                {
                    // TODO, cgillum: to recalculate the best worker again if already scaled
                    return this.GetWorkersInPriorityOrder().First();
                }

                WorkerStatus selected = null;
                var workers = await this.OnScaleOutAsync(targetCount);
                if (workers != null)
                {
                    var now = HttpScaleEnvironment.TickCount;
                    foreach (var workerName in workers)
                    {
                        var worker = this.workerList.GetOrAdd(workerName, ipAddress => new WorkerStatus(ipAddress));
                        worker.LastRequestDispatchTick = now;

                        //System.Diagnostics.Debug.WriteLine("Add {0} {1}", DateTime.UtcNow, workerName);
                        //System.Diagnostics.Debug.WriteLine(string.Empty);

                        if (selected == null)
                        {
                            selected = worker;
                        }
                    }
                }

                return selected;
            }
            finally
            {
                _scaleLock.Release();
            }

            /*
            var ipAddress = workers != null ? workers.FirstOrDefault(w => !this.workerList.ContainsKey(w)) : null;
            if (ipAddress == null)
            {
                // This is assumed to mean that there is no more capacity.
                return null;
            }

            var worker = new WorkerStatus(ipAddress);
            lock (this.workerList)
            {
                this.workerList.Add(ipAddress, worker);
            }

            return worker;
            */
        }

        private Task<int> PingAsync(string workerName)
        {
            throw new NotImplementedException();
        }

        class WorkerStatus
        {
            public WorkerStatus(string ipAddress)
            {
                this.IPAddress = ipAddress;
                this.LastRequestDispatchTick = HttpScaleEnvironment.TickCount;
            }

            public uint LastRequestDispatchTick;
            public uint LastRequestCompleteTick;
            public uint LastRequestLatencyMs;
            public string IPAddress;
            public int PendingRequestCount;
            public uint LastPingTick;
            public uint LastPingLatency;

            // TODO, cgillum: remove pragma
#pragma warning disable 0649
            public bool IsBusy;
#pragma warning restore 0649

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
