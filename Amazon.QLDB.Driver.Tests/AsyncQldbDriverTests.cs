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

namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class AsyncQldbDriverTests
    {
        private const string TestTransactionId = "testTransactionId12345";
        private const string TestRequestId = "testId";
        private const string TestLedger = "ledgerName";
        private static AsyncQldbDriverBuilder builder;
        private static MockSessionClient mockClient;
        private static AsyncQldbDriver testDriver;
        private static readonly byte[] digest = { 89, 49, 253, 196, 209, 176, 42, 98, 35, 214, 6, 163, 93,
            141, 170, 92, 75, 218, 111, 151, 173, 49, 57, 144, 227, 72, 215, 194, 186, 93, 85, 108,
        };

        [TestInitialize]
        public void SetupTest()
        {
            var sendCommandResponse = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                StartTransaction = new StartTransactionResult
                {
                    TransactionId = "testTransactionIdddddd"
                },
                ExecuteStatement = new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null,
                        Values = new List<ValueHolder>()
                    }
                },
                CommitTransaction = new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = TestRequestId
                }
            };

            mockClient = new MockSessionClient();
            mockClient.SetDefaultResponse(sendCommandResponse);

            builder = AsyncQldbDriver.Builder()
                .WithLedger("testLedger")
                .WithRetryLogging()
                .WithLogger(NullLogger.Instance)
                .WithAWSCredentials(new Mock<AWSCredentials>().Object)
                .WithQLDBSessionConfig(new AmazonQLDBSessionConfig());
            
            testDriver = new AsyncQldbDriver(TestLedger, mockClient, 4, NullLogger.Instance);
        }

        [TestMethod]
        public void TestAsyncBuilderGetsANotNullObject()
        {
            Assert.IsNotNull(builder);
        }

        [TestMethod]
        public void TestAsyncWithPoolLimitArgumentBounds()
        {
            AsyncQldbDriver driver;

            // Default pool limit
            driver = builder.Build();
            Assert.IsNotNull(driver);

            // Negative pool limit
            Assert.ThrowsException<ArgumentException>(() => builder.WithMaxConcurrentTransactions(-4));

            driver = builder.WithMaxConcurrentTransactions(0).Build();
            Assert.IsNotNull(driver);
            driver = builder.WithMaxConcurrentTransactions(4).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestAsyncQldbDriverConstructorReturnsValidObject()
        {
            var driver = new AsyncQldbDriver(TestLedger, mockClient, 4, NullLogger.Instance);

            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public async Task TestAsyncListTableNamesLists()
        {
            var factory = new ValueFactory();
            var tables = new List<string> { "table1", "table2" };
            var ions = tables.Select(t => TestingUtilities.CreateValueHolder(factory.NewString(t))).ToList();

            var h1 = QldbHash.ToQldbHash(TestTransactionId);
            h1 = AsyncTransaction.Dot(h1, QldbDriverBase<AsyncQldbSession>.TableNameQuery, new List<IIonValue>());

            mockClient.QueueResponse(TestingUtilities.StartSessionResponse(TestRequestId));
            mockClient.QueueResponse(TestingUtilities.StartTransactionResponse(TestTransactionId, TestRequestId));
            mockClient.QueueResponse(TestingUtilities.ExecuteResponse(TestRequestId, ions));
            mockClient.QueueResponse(TestingUtilities.CommitResponse(TestTransactionId, TestRequestId, h1.Hash));

            var result = await testDriver.ListTableNames();

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(tables, result.ToList());

            mockClient.Clear();
        }

        [TestMethod]
        public async Task TestAsyncGetSession_NewSessionReturned()
        {
            var driver = new AsyncQldbDriver(TestLedger, mockClient, 1, NullLogger.Instance);

            AsyncQldbSession returnedSession = await driver.GetSession();
            Assert.IsNotNull(returnedSession);
        }

        [TestMethod]
        public async Task TestAsyncGetSession_ExpectedSessionReturned()
        {
            var driver = new AsyncQldbDriver(TestLedger, mockClient, 1, NullLogger.Instance);
            AsyncQldbSession returnedSession = await driver.GetSession();

            Assert.AreEqual(TestRequestId, returnedSession.GetSessionId());
        }

        [TestMethod]
        public async Task TestAsyncGetSession_FailedToCreateSession_ThrowTheOriginalException()
        {
            var exception = new AmazonServiceException("test");
            mockClient.QueueResponse(exception);

            var driver = new AsyncQldbDriver(TestLedger, mockClient, 1, NullLogger.Instance);

            try
            {
                await driver.GetSession();
                
                Assert.Fail("driver.GetSession() should have thrown retriable exception");
            }
            catch (RetriableException re)
            {
                Assert.IsNotNull(re.InnerException);
                Assert.AreEqual(exception, re.InnerException);
            }
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithActionLambdaCanInvokeSuccessfully()
        {
            try
            {
                await testDriver.Execute(async txn => await txn.Execute("testStatement"));
            }
            catch (Exception)
            {
                Assert.Fail("driver.Execute() should not have thrown exception");
            }
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithActionAndRetryPolicyCanInvokeSuccessfully()
        {
            try
            {
                await testDriver.Execute(async txn => await txn.Execute("testStatement"),
                    Amazon.QLDB.Driver.RetryPolicy.Builder().Build());
            }
            catch (Exception)
            {
                Assert.Fail("driver.Execute() should not have thrown exception");
            }
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithFuncLambdaReturnsFuncOutput()
        {
            var result = await testDriver.Execute(txn =>
            {
                txn.Execute("testStatement");
                return Task.FromResult("testReturnValue");
            });
            Assert.AreEqual("testReturnValue", result);
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithFuncLambdaAndRetryPolicyReturnsFuncOutput()
        {
            testDriver.Dispose();
            await Assert.ThrowsExceptionAsync<QldbDriverException>(async () => await testDriver.Execute(async txn =>
            {
                await txn.Execute("testStatement");
                return Task.FromResult("testReturnValue");
            }, Amazon.QLDB.Driver.RetryPolicy.Builder().Build()));
        }

        [DataTestMethod]
        [DynamicData(nameof(CreateRetriableExecuteTestData), DynamicDataSourceType.Method)]
        public async Task TestAsyncExecute_RetryOnExceptions(
            Driver.RetryPolicy policy,
            IList<Exception> exceptions,
            Type expectedExceptionType)
        {
            string statement = "DELETE FROM table;";
            var h1 = QldbHash.ToQldbHash(TestTransactionId);
            h1 = Transaction.Dot(h1, statement, new List<IIonValue> { });

            mockClient.QueueResponse(TestingUtilities.StartSessionResponse(TestRequestId));
            mockClient.QueueResponse(TestingUtilities.StartTransactionResponse(TestTransactionId, TestRequestId));
            foreach (var ex in exceptions)
            {
                mockClient.QueueResponse(ex);

                // OccConflictException reuses the session so no need for another start session.
                if (ex is not OccConflictException)
                {
                    mockClient.QueueResponse(TestingUtilities.StartSessionResponse(TestRequestId));
                }

                mockClient.QueueResponse(TestingUtilities.StartTransactionResponse(TestTransactionId, TestRequestId));
            }
            mockClient.QueueResponse(TestingUtilities.ExecuteResponse(TestRequestId, null));
            mockClient.QueueResponse(TestingUtilities.CommitResponse(TestTransactionId, TestRequestId, h1.Hash));

            try
            {
                await testDriver.Execute(txn => txn.Execute(statement), policy);

                Assert.IsNull(expectedExceptionType);
            }
            catch (Exception e)
            {
                Assert.IsNotNull(expectedExceptionType);
                Assert.IsInstanceOfType(e, expectedExceptionType);
            }

            mockClient.Clear();
        }

        public static IEnumerable<object[]> CreateRetriableExecuteTestData()
        {
            var defaultPolicy = Driver.RetryPolicy.Builder().Build();
            var customerPolicy = Driver.RetryPolicy.Builder().WithMaxRetries(10).Build();

            var capacityExceeded = new CapacityExceededException("qldb", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable);
            var occConflict = new OccConflictException("qldb", new BadRequestException("oops"));
            var invalidSession = new InvalidSessionException("invalid session");
            var http500 = new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable);

            return new List<object[]>() {
                // No exception, No retry.
                new object[] { defaultPolicy, new Exception[0], null },
                // Generic Driver exception.
                new object[] { defaultPolicy, new Exception[] { new QldbDriverException("generic") },
                    typeof(QldbDriverException) },
                // Not supported Txn exception.
                new object[] { defaultPolicy, new Exception[] { new QldbTransactionException("txnid1111111",
                    new QldbDriverException("qldb")) }, typeof(QldbTransactionException) },
                // Not supported exception.
                new object[] { defaultPolicy, new Exception[] { new ArgumentException("qldb") },
                    typeof(ArgumentException) },
                // Transaction expiry.
                new object[] { defaultPolicy,
                    new Exception[] { new InvalidSessionException("Transaction 324weqr2314 has expired") },
                    typeof(InvalidSessionException) },
                // Retry OCC within retry limit.
                new object[] { defaultPolicy, new Exception[] { occConflict, occConflict, occConflict }, null },
                // Retry ISE within retry limit.
                new object[] { defaultPolicy, new Exception[] { invalidSession, invalidSession, invalidSession }, null },
                // Retry mixed exceptions within retry limit.
                new object[] { defaultPolicy, new Exception[] { invalidSession, occConflict, http500 }, null },
                // Retry OCC exceed limit.
                new object[] { defaultPolicy, new Exception[] { occConflict, invalidSession, http500, invalidSession,
                    occConflict }, typeof(OccConflictException) },
                // Retry CapacityExceededException exceed limit.
                new object[] { defaultPolicy, new Exception[] { capacityExceeded, capacityExceeded, capacityExceeded,
                    capacityExceeded, capacityExceeded }, typeof(CapacityExceededException) },
                // Retry customized policy within retry limit.
                new object[] { customerPolicy, new Exception[] { invalidSession, invalidSession, invalidSession, 
                    invalidSession, invalidSession, invalidSession, invalidSession, invalidSession}, null },
            };
        }
    }
}
