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

        [TestInitialize]
        public void SetupTest()
        {
            mockClient = new MockSessionClient();
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

            var sendCommandResponseWithStartSession = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            var sendCommandResponseStartTransaction = new SendCommandResponse
            {
                StartTransaction = new StartTransactionResult
                {
                    TransactionId = TestTransactionId
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            var sendCommandResponseExecute = new SendCommandResponse
            {
                ExecuteStatement = new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null,
                        Values = ions
                    }
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            var sendCommandResponseCommit = new SendCommandResponse
            {
                CommitTransaction = new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(h1.Hash),
                    TransactionId = TestTransactionId
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            
            mockClient.QueueResponse(sendCommandResponseWithStartSession);
            mockClient.QueueResponse(sendCommandResponseStartTransaction);
            mockClient.QueueResponse(sendCommandResponseExecute);
            mockClient.QueueResponse(sendCommandResponseCommit);

            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);

            var result = driver.ListTableNames();

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(tables, result.ToList());
            
            mockClient.Clear();
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
        [DynamicData(nameof(CreateDriverExceptions), DynamicDataSourceType.Method)]
        public void TestExecuteExceptionOnExecuteShouldOnlyRetryOnISEAndTAOE(Exception exception, bool expectThrow)
        {
            var statement = "DELETE FROM table;";
            var h1 = QldbHash.ToQldbHash(TestTransactionId);
            h1 = Transaction.Dot(h1, statement, new List<IIonValue> { });

            var sendCommandResponseWithStartSession = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            
            var sendCommandResponseWithStartTransaction = new SendCommandResponse
            {
                StartTransaction = new StartTransactionResult
                {
                    TransactionId = TestTransactionId
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };

            var sendCommandResponseExecute = new SendCommandResponse
            {
                ExecuteStatement = new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null
                    }
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };

            var sendCommandResponseCommit = new SendCommandResponse
            {
                CommitTransaction = new CommitTransactionResult
                {
                    TransactionId = TestTransactionId,
                    CommitDigest = new MemoryStream(h1.Hash),
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };

            // Prepare first execution attempt which fails with an exception.
            mockClient.QueueResponse(sendCommandResponseWithStartSession);
            mockClient.QueueResponse(sendCommandResponseWithStartTransaction);
            mockClient.QueueResponse(exception);
            
            // Prepare second execution attempt which succeeds.
            
            // OccConflictException does not do another start session upon retry.
            if (exception is not OccConflictException)
            {
                mockClient.QueueResponse(sendCommandResponseWithStartSession);
            }
            mockClient.QueueResponse(sendCommandResponseWithStartTransaction);
            mockClient.QueueResponse(sendCommandResponseExecute);
            mockClient.QueueResponse(sendCommandResponseCommit);

            var driver = new QldbDriver("ledgerName", mockClient, 4, NullLogger.Instance);

            try
            {
                driver.Execute(txn => txn.Execute(statement));
            }
            catch (Exception e)
            {
                Assert.IsTrue(expectThrow);
                Assert.AreEqual(exception, e);
                return;
            }

            Assert.IsFalse(expectThrow);
            
            mockClient.Clear();
        }

        public static IEnumerable<object[]> CreateDriverExceptions()
        {
            return new List<object[]>() {
                new object[] { new InvalidSessionException("invalid session"), false },
                new object[] { new OccConflictException("occ"), false },
                new object[] { new CapacityExceededException("capacity exceeded", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable), false },
                new object[] { new ArgumentException(), true },
                new object[] { new QldbDriverException("generic"), true },
            };
        }
    }
}
