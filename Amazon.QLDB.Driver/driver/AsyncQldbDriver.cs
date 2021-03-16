/*
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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>Represents a factory for accessing a specific ledger within QLDB. This class or
    /// <see cref="QldbDriver"/> should be the main entry points to any interaction with QLDB.</para>
    ///
    /// <para>
    /// This factory pools sessions and attempts to return unused but available sessions when getting new sessions.
    /// The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
    /// amount of connections the session client allows set in the <see cref="ClientConfig"/>. <see cref="Dispose"/>
    /// should be called when this factory is no longer needed in order to clean up resources, ending all sessions in
    /// the pool.
    /// </para>
    /// </summary>
    public class AsyncQldbDriver : BaseQldbDriver, IAsyncQldbDriver
    {
        private readonly BlockingCollection<AsyncQldbSession> sessionPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbDriver"/> class.
        /// </summary>
        ///
        /// <param name="ledgerName">The ledger to create sessions to.</param>
        /// <param name="sessionClient">AWS SDK session client for QLDB.</param>
        /// <param name="maxConcurrentTransactions">The maximum number of concurrent transactions.</param>
        /// <param name="logger">The logger to use.</param>
        internal AsyncQldbDriver(
            string ledgerName,
            IAmazonQLDBSession sessionClient,
            int maxConcurrentTransactions,
            ILogger logger)
            : base(ledgerName, sessionClient, maxConcurrentTransactions, logger)
        {
            this.sessionPool = new BlockingCollection<AsyncQldbSession>(maxConcurrentTransactions);
        }

        /// <summary>
        /// Retrieve a builder object for creating a <see cref="AsyncQldbDriver"/>.
        /// </summary>
        ///
        /// <returns>The builder object for creating a <see cref="AsyncQldbDriver"/>.</returns>.
        public static AsyncQldbDriverBuilder Builder()
        {
            return new AsyncQldbDriverBuilder();
        }

        /// <summary>
        /// Close this driver and end all sessions in the current pool. No-op if already closed.
        /// </summary>
        public void Dispose()
        {
            if (!this.isClosed)
            {
                this.isClosed = true;
                while (this.sessionPool.Count > 0)
                {
                    this.sessionPool.Take().Close();
                }

                this.sessionPool.Dispose();
                this.sessionClient.Dispose();
                this.poolPermits.Dispose();
            }
        }

        /// <inheritdoc/>
        public async Task Execute(
            Func<AsyncTransactionExecutor, Task> action,
            CancellationToken cancellationToken = default)
        {
            await this.Execute(action, DefaultRetryPolicy, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task Execute(
            Func<AsyncTransactionExecutor, Task> action,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default)
        {
            await this.Execute(
                async txn =>
                {
                    await action.Invoke(txn);
                    return false;
                },
                retryPolicy,
                cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func, CancellationToken cancellationToken = default)
        {
            return await this.Execute<T>(func, DefaultRetryPolicy, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default)
        {
            if (this.isClosed)
            {
                this.logger.LogError(ExceptionMessages.DriverClosed);
                throw new QldbDriverException(ExceptionMessages.DriverClosed);
            }

            bool replaceDeadSession = false;
            int retryAttempt = 0;
            while (true)
            {
                AsyncQldbSession session = null;
                try
                {
                    if (replaceDeadSession)
                    {
                        session = await this.StartNewSession();
                    }
                    else
                    {
                        session = await this.GetSession();
                    }

                    T returnedValue = await session.Execute(func);
                    this.ReleaseSession(session);
                    return returnedValue;
                }
                catch (RetriableException re)
                {
                    retryAttempt++;

                    // If initial session is invalid, always retry once with a new session.
                    if (re.InnerException is InvalidSessionException && retryAttempt == 1)
                    {
                        this.logger.LogDebug("Initial session received from pool invalid. Retrying...");
                        replaceDeadSession = true;
                        continue;
                    }

                    // Normal retry logic.
                    if (retryAttempt > retryPolicy.MaxRetries)
                    {
                        if (re.IsSessionAlive)
                        {
                            this.ReleaseSession(session);
                        }
                        else
                        {
                            this.poolPermits.Release();
                        }

                        throw re.InnerException;
                    }

                    this.logger.LogInformation("A recoverable error has occurred. Attempting retry #{}.", retryAttempt);
                    this.logger.LogDebug(
                        "Errored Transaction ID: {}. Error cause: {}",
                        re.TransactionId,
                        re.InnerException.ToString());
                    replaceDeadSession = !re.IsSessionAlive;
                    if (replaceDeadSession)
                    {
                        this.logger.LogDebug("Replacing invalid session...");
                    }
                    else
                    {
                        this.logger.LogDebug("Retrying with a different session...");
                        this.ReleaseSession(session);
                    }

                    try
                    {
                        var backoffDelay = retryPolicy.BackoffStrategy.CalculateDelay(
                            new RetryPolicyContext(retryAttempt, re.InnerException));
                        await Task.Delay(backoffDelay, cancellationToken);
                    }
                    catch (Exception)
                    {
                        // Safeguard against semaphore leak if parameter actions throw exceptions.
                        if (replaceDeadSession)
                        {
                            this.poolPermits.Release();
                        }

                        throw;
                    }
                }
                catch (QldbTransactionException qte)
                {
                    if (qte.IsSessionAlive)
                    {
                        this.ReleaseSession(session);
                    }
                    else
                    {
                        this.poolPermits.Release();
                    }

                    throw qte.InnerException;
                }
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListTableNames(CancellationToken cancellationToken = default)
        {
            IAsyncResult result = await this.Execute(
                async txn => await txn.Execute(TableNameQuery), cancellationToken);

            return (await result.ToListAsync(cancellationToken)).Select(i => i.StringValue);
        }

        private async Task<AsyncQldbSession> GetSession()
        {
            this.logger.LogDebug(
                "Getting session. There are {} free sessions and {} available permits.",
                this.sessionPool.Count,
                this.sessionPool.BoundedCapacity - this.poolPermits.CurrentCount);

            if (await this.poolPermits.WaitAsync(DefaultTimeoutInMs))
            {
                var session = this.sessionPool.Count > 0 ? this.sessionPool.Take() : await this.StartNewSession();
                return session;
            }
            else
            {
                this.logger.LogError(ExceptionMessages.SessionPoolEmpty);
                throw new QldbDriverException(ExceptionMessages.SessionPoolEmpty);
            }
        }

        private async Task<AsyncQldbSession> StartNewSession()
        {
            try
            {
                Session session = await Session.StartSessionAsync(this.ledgerName, this.sessionClient, this.logger);
                this.logger.LogDebug("Creating new pooled session with ID {}.", session.SessionId);
                return new AsyncQldbSession(session, this.logger);
            }
            catch (Exception e)
            {
                throw new RetriableException(QldbTransactionException.DefaultTransactionId, false, e);
            }
        }

        private void ReleaseSession(AsyncQldbSession session)
        {
            this.sessionPool.Add(session);
            this.logger.LogDebug("Session returned to pool; pool size is now {}.", this.sessionPool.Count);
            this.poolPermits.Release();
        }
    }
}
