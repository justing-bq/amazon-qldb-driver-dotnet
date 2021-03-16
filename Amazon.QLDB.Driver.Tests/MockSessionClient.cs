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


namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;

    internal class MockSessionClient : IAmazonQLDBSession
    {
        private readonly Queue<SendCommandResponse> responses;

        internal MockSessionClient()
        {
            this.responses = new Queue<SendCommandResponse>();
        }

        // Not used
        public IClientConfig Config => throw new NotImplementedException();

        public void Dispose()
        {
            // Not used
            throw new NotImplementedException();
        }

        public SendCommandResponse SendCommand(SendCommandRequest request)
        {
            // Not used
            throw new NotImplementedException();
        }

        public Task<SendCommandResponse> SendCommandAsync(SendCommandRequest request,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(responses.Dequeue());
        }

        internal void QueueResponse(SendCommandResponse response)
        {
            this.responses.Enqueue(response);
        }

        internal void Clear()
        {
            this.responses.Clear();
        }
    }
}
