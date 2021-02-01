﻿/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

namespace Amazon.QLDB.Driver
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// The asynchronous implementation of the session pool.
    /// </summary>
    internal class AsyncSessionPool : BaseSessionPool
    {
        private readonly BlockingCollection<AsyncQldbSession> sessionPool;
        private readonly SemaphoreSlim poolPermits;
        private readonly Func<Session> sessionCreator;
        private readonly IAsyncRetryHandler retryHandler;
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncSessionPool"/> class.
        /// </summary>
        /// <param name="sessionCreator">The method to create a new underlying QLDB session.</param>
        /// <param name="retryHandler">Handling the retry logic of the execute call.</param>
        /// <param name="maxConcurrentTransactions">The maximum number of sessions that can be created from the pool at any one time.</param>
        /// <param name="logger">Logger to be used by this.</param>
        public AsyncSessionPool(Func<Session> sessionCreator, IAsyncRetryHandler retryHandler, int maxConcurrentTransactions, ILogger logger)
        {
            this.sessionPool = new BlockingCollection<AsyncQldbSession>(maxConcurrentTransactions);
            this.poolPermits = new SemaphoreSlim(maxConcurrentTransactions, maxConcurrentTransactions);
            this.sessionCreator = sessionCreator;
            this.retryHandler = retryHandler;
            this.logger = logger;
        }

        /// <summary>
        /// Execute a function in session pool.
        /// </summary>
        /// <typeparam name="T">The return type of the function.</typeparam>
        /// <param name="func">The function to be executed in the session pool.</param>
        /// <param name="retryPolicy">The policy on retry.</param>
        /// <param name="retryAction">The customer retry action.</param>
        /// <returns>The result from the function.</returns>
        public T Execute<T>(Func<AsyncTransactionExecutor, T> func, RetryPolicy retryPolicy, Action<int> retryAction)
        {
            AsyncQldbSession session = null;
            try
            {
                session = this.GetSession();
                return this.retryHandler.RetriableExecute(
                    () => session.Execute(func),
                    retryPolicy,
                    () => session = this.StartNewSession(),
                    () =>
                    {
                        this.poolPermits.Release();
                        session = this.GetSession();
                    },
                    retryAction);
            }
            finally
            {
                if (session != null)
                {
                    session.Release();
                }
            }
        }

        /// <summary>
        /// <para>Get a <see cref="AsyncQldbSession"/> object.</para>
        ///
        /// <para>This will attempt to retrieve an active existing session, or it will start a new session with QLDB unless the
        /// number of allocated sessions has exceeded the pool size limit.</para>
        /// </summary>
        /// <returns>The <see cref="AsyncQldbSession"/> object.</returns>
        ///
        /// <exception cref="QldbDriverException">Thrown when this driver has been disposed or timeout.</exception>
        internal AsyncQldbSession GetSession()
        {
            if (this.isClosed)
            {
                this.logger.LogError(ExceptionMessages.DriverClosed);
                throw new QldbDriverException(ExceptionMessages.DriverClosed);
            }

            this.logger.LogDebug(
                "Getting session. There are {} free sessions and {} available permits.",
                this.sessionPool.Count,
                this.sessionPool.BoundedCapacity - this.poolPermits.CurrentCount);

            if (this.poolPermits.Wait(DefaultTimeoutInMs))
            {
                try
                {
                    var session = this.sessionPool.Count > 0 ? this.sessionPool.Take() : null;

                    if (session == null)
                    {
                        session = this.StartNewSession();
                        this.logger.LogDebug("Creating new pooled session with ID {}.", session.GetSessionId());
                    }

                    return session;
                }
                catch (Exception e)
                {
                    this.poolPermits.Release();
                    throw e;
                }
            }

            this.logger.LogError(ExceptionMessages.SessionPoolEmpty);
            throw new QldbDriverException(ExceptionMessages.SessionPoolEmpty);
        }

        private void ReleaseSession(AsyncQldbSession session)
        {
            if (session != null && session.IsAlive())
            {
                this.sessionPool.Add(session);
            }

            this.poolPermits.Release();
            this.logger.LogDebug("Session returned to pool; pool size is now {}.", this.sessionPool.Count);
        }

        private AsyncQldbSession StartNewSession()
        {
            return new AsyncQldbSession(this.sessionCreator.Invoke(), this.ReleaseSession, this.logger);
        }
    }
}
