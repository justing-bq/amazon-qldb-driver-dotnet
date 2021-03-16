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
    using System.Threading;
    using Amazon.QLDBSession;
    using Microsoft.Extensions.Logging;

    public abstract class BaseQldbDriver
    {
        internal const string TableNameQuery =
                "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'";

        private protected const int DefaultTimeoutInMs = 1;
        private protected static readonly RetryPolicy DefaultRetryPolicy = RetryPolicy.Builder().Build();

        private protected readonly string ledgerName;
        private protected readonly AmazonQLDBSessionClient sessionClient;
        private protected readonly ILogger logger;
        private protected readonly SemaphoreSlim poolPermits;
        private protected bool isClosed = false;

        internal BaseQldbDriver(
            string ledgerName,
            AmazonQLDBSessionClient sessionClient,
            int maxConcurrentTransactions,
            ILogger logger)
        {
            this.ledgerName = ledgerName;
            this.sessionClient = sessionClient;
            this.logger = logger;
            this.poolPermits = new SemaphoreSlim(maxConcurrentTransactions, maxConcurrentTransactions);
        }
    }
}
