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
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Reflection;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Utils;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    internal static class TestingUtilities
    {
        internal static ValueHolder CreateValueHolder(IIonValue ionValue)
        {
            MemoryStream stream = new MemoryStream();
            using (var writer = IonBinaryWriterBuilder.Build(stream))
            {
                ionValue.WriteTo(writer);
                writer.Finish();
            }

            var valueHolder = new ValueHolder
            {
                IonBinary = new MemoryStream(stream.GetWrittenBuffer()),
            };

            return valueHolder;
        }

        internal static ExecuteStatementResult GetExecuteResultNullStats(List<ValueHolder> valueHolderList)
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList,
                },
            };
        }

        internal static ExecuteStatementResult GetExecuteResultWithStats(
            List<ValueHolder> valueHolderList,
            IOUsage executeIO,
            TimingInformation executeTiming)
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList,
                },
                ConsumedIOs = executeIO,
                TimingInformation = executeTiming,
            };
        }

        internal static FetchPageResult GetFetchResultNullStats(List<ValueHolder> valueHolderList)
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList },
            };
        }

        internal static FetchPageResult GetFetchResultWithStats(
            List<ValueHolder> valueHolderList,
            IOUsage fetchIO,
            TimingInformation fetchTiming)
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList },
                ConsumedIOs = fetchIO,
                TimingInformation = fetchTiming,
            };
        }

        internal class CreateExceptionTestDataAttribute : Attribute, ITestDataSource
        {
            public IEnumerable<object[]> GetData(MethodInfo methodInfo)
            {
                return new List<object[]>() {
                new object[] { new CapacityExceededException("Capacity Exceeded Exception", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable),
                    typeof(RetriableException), typeof(CapacityExceededException),
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.InternalServerError),
                    typeof(RetriableException), null,
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable),
                    typeof(RetriableException), null,
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.Unauthorized),
                    typeof(QldbTransactionException), null,
                    Times.Once()},
                new object[] { new OccConflictException("occ"),
                    typeof(RetriableException), typeof(OccConflictException),
                    Times.Never()},
                new object[] { new AmazonServiceException(),
                    typeof(QldbTransactionException), typeof(AmazonServiceException),
                    Times.Once()},
                new object[] { new InvalidSessionException("invalid session"),
                    typeof(RetriableException), typeof(InvalidSessionException),
                    Times.Never()},
                new object[] { new QldbTransactionException(string.Empty, true, new BadRequestException("Bad request")),
                    typeof(QldbTransactionException), typeof(BadRequestException),
                    Times.Never()},
                new object[] { new TransactionAbortedException("testTransactionIdddddd", true),
                    typeof(TransactionAbortedException), null,
                    Times.Never()},
                new object[] { new Exception("Customer Exception"),
                    typeof(QldbTransactionException), typeof(Exception),
                    Times.Once()}
                };
            }

            public string GetDisplayName(MethodInfo methodInfo, object[] data)
            {
                if (data != null)
                {
                    return string.Format(CultureInfo.CurrentCulture, "{0} ({1})", methodInfo.Name, string.Join(",", data));
                }
                return null;
            }
        }
    }
}