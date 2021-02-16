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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.Runtime;

    /// <summary>
    /// Asynchronous transaction object used within lambda executions to provide a reduced view that allows only the operations that are
    /// valid within the context of an active managed transaction.
    /// </summary>
    public class AsyncTransactionExecutor : IAsyncExecutable
    {
        private readonly AsyncTransaction transaction;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncTransactionExecutor"/> class.
        /// </summary>
        ///
        /// <param name="transaction">The <see cref="AsyncTransaction"/> object the <see cref="AsyncTransactionExecutor"/> wraps.</param>
        internal AsyncTransactionExecutor(AsyncTransaction transaction)
        {
            this.transaction = transaction;
        }

        /// <summary>
        /// Asynchronously execute the statement against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public async Task<IAsyncResult> Execute(string statement, CancellationToken cancellationToken = default)
        {
            return await this.transaction.Execute(statement, cancellationToken);
        }

        /// <summary>
        /// Asynchronously execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public async Task<IAsyncResult> Execute(
            string statement, List<IIonValue> parameters, CancellationToken cancellationToken = default)
        {
            return await this.transaction.Execute(statement, parameters, cancellationToken);
        }

        /// <summary>
        /// Asynchronously execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public async Task<IAsyncResult> Execute(
            string statement, CancellationToken cancellationToken = default, params IIonValue[] parameters)
        {
            return await this.transaction.Execute(statement, cancellationToken, parameters);
        }

        /// <summary>
        /// Asynchronously abort the transaction and roll back any changes.
        /// </summary>
        ///
        /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
        ///
        internal async void Abort(CancellationToken cancellationToken = default)
        {
            try
            {
                await this.transaction.Abort(cancellationToken);
                throw new TransactionAbortedException(this.transaction.Id, true);
            }
            catch (AmazonServiceException ase)
            {
                throw new TransactionAbortedException(this.transaction.Id, false, ase);
            }
        }
    }
}
