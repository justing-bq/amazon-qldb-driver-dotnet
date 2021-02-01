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
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Base class for QLDB Session
    /// </summary>
    internal abstract class BaseQldbSession
    {
        protected readonly ILogger logger;
        protected Session session;
        protected bool isAlive;

        public bool IsAlive()
        {
            return this.isAlive;
        }

        /// <summary>
        /// Close the internal session object.
        /// </summary>
        public void Close()
        {
            this.session.End();
        }

        /// <summary>
        /// Release the session which still can be used by another transaction.
        /// </summary>
        public abstract void Release();

        /// <summary>
        /// Retrieve the ID of this session..
        /// </summary>
        ///
        /// <returns>The ID of this session.</returns>
        internal string GetSessionId()
        {
            return this.session.SessionId;
        }
    }
}
