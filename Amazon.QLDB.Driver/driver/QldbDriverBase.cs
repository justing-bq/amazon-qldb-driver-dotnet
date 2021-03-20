﻿namespace Amazon.QLDB.Driver
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    internal class QldbDriverBase<T> : IDisposable
        where T : BaseQldbSession
    {
        internal const string TableNameQuery =
                "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'";

        internal const int DefaultTimeoutInMs = 1;
        internal static readonly RetryPolicy DefaultRetryPolicy = RetryPolicy.Builder().Build();

        internal readonly string LedgerName;
        internal readonly IAmazonQLDBSession SessionClient;
        internal readonly ILogger Logger;
        internal SemaphoreSlim PoolPermits;
        internal BlockingCollection<T> SessionPool;
        internal bool IsClosed = false;

        internal QldbDriverBase(
            string ledgerName,
            IAmazonQLDBSession sessionClient,
            int maxConcurrentTransactions,
            ILogger logger)
        {
            this.LedgerName = ledgerName;
            this.SessionClient = sessionClient;
            this.Logger = logger;
            this.PoolPermits = new SemaphoreSlim(maxConcurrentTransactions, maxConcurrentTransactions);
            this.SessionPool = new BlockingCollection<T>(maxConcurrentTransactions);
        }

        public void Dispose()
        {
            if (!this.IsClosed)
            {
                this.IsClosed = true;
                while (this.SessionPool.Count > 0)
                {
                    this.SessionPool.Take().Close();
                }

                this.SessionPool.Dispose();
                this.SessionClient.Dispose();
                this.PoolPermits.Dispose();
            }
        }

        internal void ReleaseSession(T session)
        {
            this.SessionPool.Add(session);
            this.Logger.LogDebug("Session returned to pool; pool size is now {}.", this.SessionPool.Count);
            this.PoolPermits.Release();
        }

        internal void HandleRetry(
            QldbTransactionException qte,
            T currentSession,
            int maxRetries,
            ref bool replaceDeadSession,
            ref int retryAttempt)
        {
            if (qte is RetriableException)
            {
                retryAttempt++;

                // If initial session is invalid, always retry once with a new session.
                if (qte.InnerException is InvalidSessionException && retryAttempt == 1)
                {
                    this.Logger.LogDebug("Initial session received from pool invalid. Retrying...");
                    replaceDeadSession = true;
                    return;
                }

                // Normal retry logic.
                if (retryAttempt > maxRetries)
                {
                    if (qte.IsSessionAlive)
                    {
                        this.ReleaseSession(currentSession);
                    }
                    else
                    {
                        this.PoolPermits.Release();
                    }

                    throw qte.InnerException;
                }

                this.Logger.LogInformation("A recoverable error has occurred. Attempting retry #{}.", retryAttempt);
                this.Logger.LogDebug(
                    "Errored Transaction ID: {}. Error cause: {}",
                    qte.TransactionId,
                    qte.InnerException.ToString());
                replaceDeadSession = !qte.IsSessionAlive;
                if (replaceDeadSession)
                {
                    this.Logger.LogDebug("Replacing invalid session...");
                }
                else
                {
                    this.Logger.LogDebug("Retrying with a different session...");
                    this.ReleaseSession(currentSession);
                }
            }
            else
            {
                if (qte.IsSessionAlive)
                {
                    this.ReleaseSession(currentSession);
                }
                else
                {
                    this.PoolPermits.Release();
                }

                throw qte.InnerException;
            }
        }
    }
}
