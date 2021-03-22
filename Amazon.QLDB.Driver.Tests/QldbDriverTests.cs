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
    using System.IO;
    using System.Linq;
    using System.Net;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class QldbDriverTests
    {
        private const string TestTransactionId = "testTransactionId12345";
        private const string TestRequestId = "testId";
        private const string TestLedger = "ledgerName";
        private static QldbDriverBuilder builder;
        private static MockSessionClient mockClient;
        private static QldbDriver testDriver;
        private static readonly byte[] digest = new byte[] { 89, 49, 253, 196, 209, 176, 42, 98, 35, 214, 6, 163, 93,
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

            builder = QldbDriver.Builder()
                .WithLedger("testLedger")
                .WithRetryLogging()
                .WithLogger(NullLogger.Instance)
                .WithAWSCredentials(new Mock<AWSCredentials>().Object)
                .WithQLDBSessionConfig(new AmazonQLDBSessionConfig());

            testDriver = new QldbDriver(TestLedger, mockClient, 4, NullLogger.Instance);
        }

        [TestMethod]
        public void TestBuilderGetsANotNullObject()
        {
            Assert.IsNotNull(builder);
        }

        [TestMethod]
        public void TestWithPoolLimitArgumentBounds()
        {
            QldbDriver driver;

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
        public void TestQldbDriverConstructorReturnsValidObject()
        {
            var driver = new QldbDriver(TestLedger, mockClient, 4, NullLogger.Instance);

            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestListTableNamesLists()
        {
            var factory = new ValueFactory();
            var tables = new List<string>() { "table1", "table2" };
            var ions = tables.Select(t => TestingUtilities.CreateValueHolder(factory.NewString(t))).ToList();

            var h1 = QldbHash.ToQldbHash(TestTransactionId);
            h1 = Transaction.Dot(h1, QldbDriverBase<QldbSession>.TableNameQuery, new List<IIonValue> { });

            mockClient.QueueResponse(TestingUtilities.StartSessionResponse(TestRequestId));
            mockClient.QueueResponse(TestingUtilities.StartTransactionResponse(TestTransactionId, TestRequestId));
            mockClient.QueueResponse(TestingUtilities.ExecuteResponse(TestRequestId, ions));
            mockClient.QueueResponse(TestingUtilities.CommitResponse(TestTransactionId, TestRequestId, h1.Hash));

            var result = testDriver.ListTableNames();

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(tables, result.ToList());

            mockClient.Clear();
        }

        [TestMethod]
        public void TestGetSession_NewSessionReturned()
        {
            QldbSession returnedSession = testDriver.GetSession();
            Assert.IsNotNull(returnedSession);
        }

        [TestMethod]
        public void TestGetSession_ExpectedSessionReturned()
        {
            QldbSession returnedSession = testDriver.GetSession();

            Assert.AreEqual(TestRequestId, returnedSession.GetSessionId());
        }

        [TestMethod]
        public void TestGetSession_GetTwoSessionsFromPoolOfOne_TimeoutOnSecondGet()
        {
            var driver = new QldbDriver(TestLedger, mockClient, 1, NullLogger.Instance);
            QldbSession returnedSession = driver.GetSession();
            Assert.ThrowsException<QldbDriverException>(() => driver.GetSession());

            Assert.IsNotNull(returnedSession);
        }

        [TestMethod]
        public void TestGetSession_FailedToCreateSession_ThrowTheOriginalException()
        {
            var exception = new AmazonServiceException("test");
            mockClient.QueueResponse(exception);

            try
            {
                testDriver.GetSession();
                
                Assert.Fail("driver.GetSession() should have thrown retriable exception");
            }
            catch (RetriableException re)
            {
                Assert.IsNotNull(re.InnerException);
                Assert.AreEqual(exception, re.InnerException);
            }
        }

        [TestMethod]
        public void TestRetryPolicyContext_Create_ShouldReturnCorrectProperties()
        {
            var exception = new Exception();
            var retries = 3;

            var context = new RetryPolicyContext(retries, exception);

            Assert.AreEqual(retries, context.RetriesAttempted);
            Assert.AreEqual(exception, context.LastException);
        }

        [TestMethod]
        public void TestExecuteWithActionLambdaCanInvokeSuccessfully()
        {
            try
            {
                testDriver.Execute(txn => txn.Execute("testStatement"));
            }
            catch (Exception)
            {
                Assert.Fail("driver.Execute() should not have thrown exception");
            }
        }

        [TestMethod]
        public void TestExecuteWithActionAndRetryPolicyCanInvokeSuccessfully()
        {
            try
            {
                testDriver.Execute(
                    txn => txn.Execute("testStatement"),
                    Driver.RetryPolicy.Builder().Build());
            }
            catch (Exception)
            {
                Assert.Fail("driver.Execute() should not have thrown exception");
            }
        }

        [TestMethod]
        [Obsolete]
        public void TestExecuteWithActionAndRetryActionCanInvokeSuccessfully()
        {
            try
            {
                testDriver.Execute(
                    txn => txn.Execute("testStatement"),
                    Console.WriteLine);
            }
            catch (Exception)
            {
                Assert.Fail("driver.Execute() should not have thrown exception");
            }
        }

        [TestMethod]
        public void TestExecuteWithFuncLambdaReturnsFuncOutput()
        {
            var result = testDriver.Execute((txn) =>
            {
                txn.Execute("testStatement");
                return "testReturnValue";
            });
            Assert.AreEqual("testReturnValue", result);
        }

        [TestMethod]
        public void TestExecuteWithFuncLambdaAndRetryPolicyReturnsFuncOutput()
        {
            testDriver.Dispose();
            Assert.ThrowsException<QldbDriverException>(() => testDriver.Execute((txn) =>
            {
                txn.Execute("testStatement");
                return "testReturnValue";
            }, Driver.RetryPolicy.Builder().Build()));
        }

        [TestMethod]
        [Obsolete]
        public void TestExecuteWithFuncLambdaAndRetryActionReturnsFuncOutput()
        {
            testDriver.Dispose();
            Assert.ThrowsException<QldbDriverException>(() => testDriver.Execute((txn) =>
            {
                txn.Execute("testStatement");
                return "testReturnValue";
            }, Console.WriteLine));
        }

        [DataTestMethod]
        [DynamicData(nameof(CreateRetriableExecuteTestData), DynamicDataSourceType.Method)]
        public void TestExecute_RetryOnExceptions(
            Driver.RetryPolicy policy,
            IList<Exception> exceptions,
            Type expectedExceptionType,
            Times retryActionCalledTimes)
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

            var retry = new Mock<Action<int>>();

            try
            {
                testDriver.Execute(txn => txn.Execute(statement), policy, retry.Object);

                Assert.IsNull(expectedExceptionType);
            }
            catch (Exception e)
            {
                Assert.IsNotNull(expectedExceptionType);
                Assert.IsInstanceOfType(e, expectedExceptionType);
            }

            retry.Verify(r => r.Invoke(It.IsAny<int>()), retryActionCalledTimes);

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
                new object[] { defaultPolicy, new Exception[0], null, Times.Never() },
                // Generic Driver exception.
                new object[] { defaultPolicy, new Exception[] { new QldbDriverException("generic") },
                    typeof(QldbDriverException), Times.Never() },
                // Not supported Txn exception.
                new object[] { defaultPolicy, new Exception[] { new QldbTransactionException("txnid1111111",
                    new QldbDriverException("qldb")) }, typeof(QldbTransactionException), Times.Never() },
                // Not supported exception.
                new object[] { defaultPolicy, new Exception[] { new ArgumentException("qldb") },
                    typeof(ArgumentException), Times.Never() },
                // Transaction expiry.
                new object[] { defaultPolicy,
                    new Exception[] { new InvalidSessionException("Transaction 324weqr2314 has expired") },
                    typeof(InvalidSessionException), Times.Never() },
                // Retry OCC within retry limit.
                new object[] { defaultPolicy, new Exception[] { occConflict, occConflict, occConflict }, null,
                    Times.Exactly(3) },
                // Retry ISE within retry limit.
                new object[] { defaultPolicy, new Exception[] { invalidSession, invalidSession, invalidSession }, null,
                    Times.Exactly(3) },
                // Retry mixed exceptions within retry limit.
                new object[] { defaultPolicy, new Exception[] { invalidSession, occConflict, http500 }, null,
                    Times.Exactly(3) },
                // Retry OCC exceed limit.
                new object[] { defaultPolicy, new Exception[] { occConflict, invalidSession, http500, invalidSession, 
                    occConflict }, typeof(OccConflictException), Times.Exactly(4) },
                // Retry CapacityExceededException exceed limit.
                new object[] { defaultPolicy, new Exception[] { capacityExceeded, capacityExceeded, capacityExceeded,
                    capacityExceeded, capacityExceeded }, typeof(CapacityExceededException), Times.Exactly(4) },
                // Retry customized policy within retry limit.
                new object[] { customerPolicy, new Exception[] { invalidSession, invalidSession, invalidSession, 
                    invalidSession, invalidSession, invalidSession, invalidSession, invalidSession}, null,
                    Times.Exactly(8) },
            };
        }
    }
}
