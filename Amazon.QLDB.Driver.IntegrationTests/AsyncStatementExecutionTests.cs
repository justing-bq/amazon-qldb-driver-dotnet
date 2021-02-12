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

namespace Amazon.QLDB.Driver.AsyncIntegrationTests
{
    using Amazon.QLDB.Driver.AsyncIntegrationTests.utils;
    using Amazon.QLDB.Driver.IntegrationTests.utils;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;

    [TestClass]
    public class StatementExecutionTests
    {
        private static readonly IValueFactory ValueFactory = new ValueFactory();
        private static AmazonQLDBSessionConfig amazonQldbSessionConfig;
        private static AsyncIntegrationTestBase integrationTestBase;
        private static AsyncQldbDriver qldbDriver;

        [ClassInitialize]
        public async static void SetUp(TestContext context)
        {
            // Get AWS configuration properties from .runsettings file.
            string region = context.Properties["region"].ToString();

            amazonQldbSessionConfig = AsyncIntegrationTestBase.CreateAmazonQLDBSessionConfig(region);
            integrationTestBase = new AsyncIntegrationTestBase(Constants.LedgerName, region);

            integrationTestBase.RunForceDeleteLedger();

            integrationTestBase.RunCreateLedger();
            qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig);

            // Create table.
            var query = $"CREATE TABLE {Constants.TableName}";

            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });


            Assert.AreEqual(1, count);

            var result = await qldbDriver.ListTableNamesAsync();
            foreach (var row in result)
            {
                Assert.AreEqual(Constants.TableName, row);
            }
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            integrationTestBase.RunDeleteLedger();
            qldbDriver.Dispose();
        }

        [TestCleanup]
        public async void TestCleanup()
        {
            // Delete all documents in table.
            await qldbDriver.Execute(txn => txn.Execute($"DELETE FROM {Constants.TableName}"));
        }

        [TestMethod]
        public async void Execute_DropExistingTable_TableDropped()
        {
            // Given.
            var create_table_query = $"CREATE TABLE {Constants.CreateTableName}";
            var create_table_count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(create_table_query);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, create_table_count);

            // Execute ListTableNames() to ensure table is created.
            var result = await qldbDriver.ListTableNamesAsync();

            var tables = new List<string>();
            foreach (var row in result)
            {
                tables.Add(row);
            }
            Assert.IsTrue(tables.Contains(Constants.CreateTableName));

            // When.
            var drop_table_query = $"DROP TABLE {Constants.CreateTableName}";
            var drop_table_count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(drop_table_query);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, drop_table_count);

            // Then.
            tables.Clear();
            var updated_tables_result = await qldbDriver.ListTableNamesAsync();
            foreach (var row in updated_tables_result)
            {
                tables.Add(row);
            }
            Assert.IsFalse(tables.Contains(Constants.CreateTableName));
        }

    }
}
