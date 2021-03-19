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
        private static QldbDriverBuilder builder;
        private static MockSessionClient mockClient;
        private static readonly byte[] digest = new byte[] { 89, 49, 253, 196, 209, 176, 42, 98, 35, 214, 6, 163, 93,
            141, 170, 92, 75, 218, 111, 151, 173, 49, 57, 144, 227, 72, 215, 194, 186, 93, 85, 108,
        };

        private static SendCommandResponse startSessionResponse = new SendCommandResponse
        {
            StartSession = new StartSessionResult { SessionToken = "testToken" },
            ResponseMetadata = new ResponseMetadata { RequestId = "testId" }
        };
        private static SendCommandResponse startTransactionResponse = new SendCommandResponse
        {
            StartTransaction = new StartTransactionResult { TransactionId = TestTransactionId },
            ResponseMetadata = new ResponseMetadata { RequestId = "testId" }
        };

        private SendCommandResponse executeResponse(List<ValueHolder> values)
        {
            Page firstPage;
            if (values == null)
            {
                firstPage = new Page {NextPageToken = null};
            }
            else
            {
                firstPage = new Page {NextPageToken = null, Values = values};
            }
            return new SendCommandResponse
            {
                ExecuteStatement = new ExecuteStatementResult { FirstPage = firstPage },
                ResponseMetadata = new ResponseMetadata { RequestId = "testId" }
            };
        }

        private SendCommandResponse commitResponse(byte[] hash)
        {
            return new SendCommandResponse
            {
                CommitTransaction = new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(hash),
                    TransactionId = TestTransactionId
                },
                ResponseMetadata = new ResponseMetadata { RequestId = "testId" }
            };
        }

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
                    RequestId = "testId"
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
            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);

            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestListTableNamesLists()
        {
            var factory = new ValueFactory();
            var tables = new List<string>() { "table1", "table2" };
            var ions = tables.Select(t => TestingUtilities.CreateValueHolder(factory.NewString(t))).ToList();

            var h1 = QldbHash.ToQldbHash(TestTransactionId);
            h1 = Transaction.Dot(h1, QldbDriver.TableNameQuery, new List<IIonValue> { });

            mockClient.QueueResponse(startSessionResponse);
            mockClient.QueueResponse(startTransactionResponse);
            mockClient.QueueResponse(executeResponse(ions));
            mockClient.QueueResponse(commitResponse(h1.Hash));

            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);

            var result = driver.ListTableNames();

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(tables, result.ToList());

            mockClient.Clear();
        }

        [TestMethod]
        public void TestGetSession_NewSessionReturned()
        {
            var driver = new QldbDriver("ledgerName", mockClient, 1, NullLogger.Instance);

            QldbSession returnedSession = driver.GetSession();
            Assert.IsNotNull(returnedSession);
        }

        [TestMethod]
        public void TestGetSession_ExpectedSessionReturned()
        {
            var session = new Session(null, null, null, "testId", null);

            var driver = new QldbDriver("ledgerName", mockClient, 1, NullLogger.Instance);
            QldbSession returnedSession = driver.GetSession();

            Assert.AreEqual(session.SessionId, returnedSession.GetSessionId());
        }

        [TestMethod]
        public void TestGetSession_GetTwoSessionsFromPoolOfOne_TimeoutOnSecondGet()
        {
            var driver = new QldbDriver("ledgerName", mockClient, 1, NullLogger.Instance);
            QldbSession returnedSession = driver.GetSession();
            Assert.ThrowsException<QldbDriverException>(() => driver.GetSession());

            Assert.IsNotNull(returnedSession);
        }

        [TestMethod]
        public void TestGetSession_FailedToCreateSession_ThrowTheOriginalException()
        {
            var exception = new AmazonServiceException("test");
            mockClient.QueueResponse(exception);

            var driver = new QldbDriver("ledgerName", mockClient, 1, NullLogger.Instance);

            try
            {
                driver.GetSession();
            }
            catch (RetriableException re)
            {
                Assert.IsNotNull(re.InnerException);
                Assert.IsTrue(re.InnerException == exception);

                return;
            }

            Assert.Fail("driver.GetSession() should have thrown retriable exception");
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
            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);
            driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
            });
        }

        [TestMethod]
        public void TestExecuteWithActionAndRetryPolicyCanInvokeSuccessfully()
        {
            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);
            driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
            },
            Driver.RetryPolicy.Builder().Build());
        }

        [TestMethod]
        [Obsolete]
        public void TestExecuteWithActionAndRetryActionCanInvokeSuccessfully()
        {
            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);
            driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
            },
            Console.WriteLine);
        }

        [TestMethod]
        public void TestExecuteWithFuncLambdaReturnsFuncOutput()
        {
            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);

            var result = driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
                return "testReturnValue";
            });
            Assert.AreEqual("testReturnValue", result);
        }

        [TestMethod]
        public void TestExecuteWithFuncLambdaAndRetryPolicyReturnsFuncOutput()
        {
            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);

            driver.Dispose();
            Assert.ThrowsException<QldbDriverException>(() => driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
                return "testReturnValue";
            }, Driver.RetryPolicy.Builder().Build()));
        }

        [TestMethod]
        [Obsolete]
        public void TestExecuteWithFuncLambdaAndRetryActionReturnsFuncOutput()
        {
            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);

            driver.Dispose();
            Assert.ThrowsException<QldbDriverException>(() => driver.Execute((txn) =>
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

            mockClient.QueueResponse(startSessionResponse);
            mockClient.QueueResponse(startTransactionResponse);
            foreach (var ex in exceptions)
            {
                mockClient.QueueResponse(ex);

                // OccConflictException reuses the session so no need for another start session.
                if (ex is not OccConflictException)
                {
                    mockClient.QueueResponse(startSessionResponse);
                }

                mockClient.QueueResponse(startTransactionResponse);
            }
            mockClient.QueueResponse(executeResponse(null));
            mockClient.QueueResponse(commitResponse(h1.Hash));

            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);
            var retry = new Mock<Action<int>>();

            try
            {
                driver.Execute(txn => txn.Execute(statement), policy, retry.Object);

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

            var cee = new CapacityExceededException("qldb", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable);
            var occ = new OccConflictException("qldb", new BadRequestException("oops"));
            var ise = new InvalidSessionException("invalid session");
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
                new object[] { defaultPolicy, new Exception[] { occ, occ, occ }, null, Times.Exactly(3) },
                // Retry ISE within retry limit.
                new object[] { defaultPolicy, new Exception[] { ise, ise, ise }, null, Times.Exactly(2) },
                // Retry mixed exceptions within retry limit.
                new object[] { defaultPolicy, new Exception[] { ise, occ, http500 }, null, Times.Exactly(2) },
                // Retry OCC exceed limit.
                new object[] { defaultPolicy, new Exception[] { occ, ise, http500, ise, occ },
                    typeof(OccConflictException), Times.Exactly(4) },
                // Retry CapacityExceededException exceed limit.
                new object[] { defaultPolicy, new Exception[] { cee, cee, cee, cee, cee },
                    typeof(CapacityExceededException), Times.Exactly(4) },
                // Retry customized policy within retry limit.
                new object[] { customerPolicy, new Exception[] { ise, ise, ise, ise, ise, ise, ise, ise}, null,
                    Times.Exactly(7) },
            };
        }
    }
}
