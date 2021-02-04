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
    using System.Threading;
    using System.Threading.Tasks;

    internal class AsyncRetryHandler : IAsyncRetryHandler
    {
        public async Task<T> RetriableExecute<T>(
            Func<Task<T>> func,
            RetryPolicy retryPolicy,
            Func<Task> newSessionAction,
            Func<Task> nextSessionAction,
            Action<int> retryAction,
            CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }
    }
}