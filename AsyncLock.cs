//------------------------------------------------------------------------------
// <copyright file="AsyncLock.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Web.Hosting.RewriteProvider.HttpScale
{
    /// <summary>
    /// Fast asynchronous lock that does zero allocations in the synchronous case.
    /// </summary>
    sealed class AsyncLock : IDisposable
    {
        private readonly SemaphoreSlim semaphore;
        private readonly Task<Releaser> releaser;

        public AsyncLock()
        {
            this.semaphore = new SemaphoreSlim(1);
            this.releaser = Task.FromResult(new Releaser(this));
        }

        public Task<Releaser> LockAsync(int timeoutMs)
        {
            Task<bool> wait = this.semaphore.WaitAsync(timeoutMs);
            if (wait.IsCompleted)
            {
                return this.releaser;
            }

            return wait.ContinueWith<Releaser>(
                OnCompletedAsync,
                this,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        public void Dispose()
        {
            this.semaphore.Dispose();
        }

        private static Releaser OnCompletedAsync(Task<bool> task, object state)
        {
            if (task.Result)
            {
                return new Releaser((AsyncLock)state);
            }

            return new Releaser(timedOut: true);
        }

        public struct Releaser : IDisposable
        {
            private readonly AsyncLock asyncLock;
            private readonly bool timedOut;

            internal Releaser(AsyncLock asyncLock)
            {
                this.asyncLock = asyncLock;
                this.timedOut = false;
            }

            internal Releaser(bool timedOut)
            {
                this.timedOut = timedOut;
                this.asyncLock = null;
            }

            public bool TimedOut
            {
                get { return this.timedOut; }
            }

            public void Dispose()
            {
                if (!this.timedOut)
                {
                    this.asyncLock.semaphore.Release();
                }
            }
        }
    }
}
