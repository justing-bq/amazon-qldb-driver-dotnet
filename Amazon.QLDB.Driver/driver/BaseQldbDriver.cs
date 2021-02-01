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
    /// <summary>
    /// Base class for QLDB Drivers.
    /// </summary>
    public abstract class BaseQldbDriver
    {
        internal const string TableNameQuery =
            "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'";

        protected static readonly RetryPolicy DefaultRetryPolicy = RetryPolicy.Builder().Build();
        internal readonly SessionPool sessionPool;

        /// <summary>
        /// Close this driver and end all sessions in the current pool. No-op if already closed.
        /// </summary>
        public void Dispose()
        {
            this.sessionPool.Dispose();
        }
    }
}
