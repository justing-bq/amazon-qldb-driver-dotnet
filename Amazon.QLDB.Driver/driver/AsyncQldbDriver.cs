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
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class AsyncQldbDriver
    {
        public Task<IEnumerable<string>> ListTableNames()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public Task Execute(Action<AsyncTransactionExecutor> action)
        {
            throw new NotImplementedException();
        }

        public Task Execute(Action<AsyncTransactionExecutor> action, RetryPolicy retryPolicy)
        {
            throw new NotImplementedException();
        }

        public Task<T> Execute<T>(Func<AsyncTransactionExecutor, Task<T>> func)
        {
            throw new NotImplementedException();
        }

        public Task<T> Execute<T>(Func<AsyncTransactionExecutor, Task<T>> func, RetryPolicy retryPolicy)
        {
            throw new NotImplementedException();
        }

        internal Task<T> Execute<T>(Func<AsyncTransactionExecutor, Task<T>> func, RetryPolicy retryPolicy, Action<int> retryAction)
        {
            throw new NotImplementedException();
        }
    }
}
