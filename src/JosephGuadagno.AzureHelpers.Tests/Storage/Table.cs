using System;
using System.Net;
using JosephGuadagno.AzureHelpers.Tests.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;

namespace JosephGuadagno.AzureHelpers.Tests.Storage
{
    [TestClass]
    public class Table
    {
        private const string CreateMe = "CreateMe";
        private const string CreateMe2 = "CreateMe2";
        private const string CreateMe3 = "CreateMe3";
        private const string CreateMe4 = "CreateMe4";
        private const string DoNotCreateMe = "DoNotCreateMe";
        private const string NonExistingTable = "NonExistingTable";
        private const string ExistingTable = "ExistingTable";
        private static AzureHelpers.Storage.AzureTables _azureTables;

        private const string PartitionKey = "PartitionKey";
        private const string RowKey = "RowKey";
        private TestTableEntity _testTableEntity = new TestTableEntity(PartitionKey, RowKey) {Property1 = "Property1", Property2 = "Property2"};

        [ClassInitialize]
        public static void TestsSetup(TestContext testContext)
        {
            _azureTables = new AzureHelpers.Storage.AzureTables(Constants.ExistingValidAppSettingsTableStorageConnectionString);
            // Create a temporary table
            var cloudTableClient = _azureTables.CloudStorageAccount.CreateCloudTableClient();
            var tempTable = cloudTableClient.GetTableReference(ExistingTable);
            tempTable.CreateIfNotExists();
        }

        #region CloudTable Tests

        [TestMethod]
        public void CloudTable_DoesNotExists()
        {
            Assert.IsFalse(_azureTables.DoesCloudTableExists(NonExistingTable));
        }

        [TestMethod]
        public void CloudTable_DoesExists()
        {
            Assert.IsTrue(_azureTables.DoesCloudTableExists(ExistingTable));
        }

        [TestMethod]
        public void CreateTableIfDoesNotExist()
        {
            var cloudTable = _azureTables.CreateCloudTable(CreateMe2);
            Assert.IsNotNull(cloudTable);
            Assert.AreEqual(CreateMe2, cloudTable.Name);
        }

        [TestMethod]
        public void GetReferenceToNonExistingTableReturnsNullIfCreateIfExistsIsFalse()
        {
            var cloudTable = _azureTables.GetCloudTable(DoNotCreateMe, false);
            Assert.IsNull(cloudTable);
        }

        [TestMethod]
        public void GetReferenceToNonExistingTableReturnsTableIfCreateIfExistsIsTrue()
        {
            var cloudTable = _azureTables.GetCloudTable(CreateMe);
            Assert.IsNotNull(cloudTable);
            Assert.AreEqual(CreateMe, cloudTable.Name);
        }

        [TestMethod]
        public void GetReferenceToExistingTable()
        {
            var cloudTable = _azureTables.GetCloudTable(ExistingTable, false);
            Assert.IsNotNull(cloudTable);
            Assert.AreEqual(ExistingTable, cloudTable.Name);
        }

        public void CreateCloudTableByName()
        {
            var cloudTable = _azureTables.CreateCloudTable(CreateMe3);
            Assert.IsNotNull(cloudTable);
            Assert.IsTrue(cloudTable.Exists());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateCloudTableByNameWithNameNullExpectedNullReferenceException()
        {
            _azureTables.CreateCloudTable((string) null);
        }

        [TestMethod]
        public void CreateCloudTableByCloudTable()
        {
            var cloudTable = _azureTables.CloudTableClient.GetTableReference(CreateMe4);
            cloudTable = _azureTables.CreateCloudTable(cloudTable);
            Assert.IsNotNull(cloudTable);
            Assert.IsTrue(cloudTable.Exists());
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void CreateCloudTableByCloudTableWithCloudTableExpectedNullReferenceException()
        {
            _azureTables.CreateCloudTable((CloudTable) null);
        }

        #endregion CloudTable Tests

        #region Insert Entity 

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertEntityTableNameAsStringNullExpectsArgumentNullException()
        {
            _azureTables.InsertEntity((string) null, _testTableEntity);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertEntityTableNameAsCloudTableNullExpectsArgumentNullException()
        {
            CloudTable cloudTable = null;
            // ReSharper disable once ExpressionIsAlwaysNull
            _azureTables.InsertEntity(cloudTable, _testTableEntity);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertEntityTableEntityNullExpectsArgumentNullException()
        {
            _azureTables.InsertEntity(ExistingTable, null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertEntityTableDoesNotExistsExpectsNullReferenceException()
        {
            _azureTables.InsertEntity(ExistingTable, null);
        }

        [TestMethod]
        public void InsertEntityShouldInsert()
        {
            var results = _azureTables.InsertEntity(ExistingTable, _testTableEntity);
            Assert.AreEqual(HttpStatusCode.NoContent, results);

            RemoveEntity(ExistingTable, _testTableEntity);
        }

        #endregion Insert Entity

        #region InsertOrMerge Entity 

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertOrMergeEntityTableNameAsStringNullExpectsArgumentNullException()
        {
            _azureTables.InsertOrMergeEntity((string)null, _testTableEntity);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertOrMergeEntityTableNameAsCloudTableNullExpectsArgumentNullException()
        {
            CloudTable cloudTable = null;
            // ReSharper disable once ExpressionIsAlwaysNull
            _azureTables.InsertOrMergeEntity(cloudTable, _testTableEntity);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertOrMergeEntityTableEntityNullExpectsArgumentNullException()
        {
            _azureTables.InsertOrMergeEntity(ExistingTable, null);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void InsertOrMergeEntityTableDoesNotExistsExpectsNullReferenceException()
        {
            _azureTables.InsertOrMergeEntity(ExistingTable, null);
        }

        [TestMethod]
        public void InsertOrMergeEntityShouldInsert()
        {
            var results = _azureTables.InsertOrMergeEntity(ExistingTable, _testTableEntity);
            Assert.AreEqual(HttpStatusCode.NoContent, results);

            RemoveEntity(ExistingTable, _testTableEntity);
        }

        [TestMethod]
        public void InsertOrMergeEntityShouldMerge()
        {
            var results = _azureTables.InsertOrMergeEntity(ExistingTable, _testTableEntity);
            Assert.AreEqual(HttpStatusCode.NoContent, results);

            var mergedItem = new TestTableEntity(PartitionKey, RowKey) {Property1 = "Property1a", Property2 = "Property2a"};

            results = _azureTables.InsertOrReplaceEntity(ExistingTable, mergedItem);
            Assert.AreEqual(HttpStatusCode.NoContent, results);
            // Get a copy to compare
            var retrievedEntity = GetTableEntity(ExistingTable, PartitionKey, RowKey);
            Assert.AreEqual(retrievedEntity.PartitionKey, mergedItem.PartitionKey);
            Assert.AreEqual(retrievedEntity.RowKey, mergedItem.RowKey);
            Assert.AreEqual(retrievedEntity.Property1, mergedItem.Property1);
            Assert.AreEqual(retrievedEntity.Property2, mergedItem.Property2);

            RemoveEntity(ExistingTable, retrievedEntity);
        }

        #endregion InsertOrMerge Entity

        [ClassCleanup]
        public static void TestCleanup()
        {
            // Remove the temporarily created table.
            var cloudTableClient = _azureTables?.CloudStorageAccount.CreateCloudTableClient();
            if (cloudTableClient == null) return;

            DeleteTableIfItExists(cloudTableClient, CreateMe);
            DeleteTableIfItExists(cloudTableClient, CreateMe2);
            DeleteTableIfItExists(cloudTableClient, CreateMe3);
            DeleteTableIfItExists(cloudTableClient, CreateMe4);
            DeleteTableIfItExists(cloudTableClient, ExistingTable);

        }

        private static void DeleteTableIfItExists(CloudTableClient cloudTableClient, string tableName )
        {
            var cloudTable = cloudTableClient.GetTableReference(tableName);
            if (cloudTable != null && cloudTable.Exists())
            {
                cloudTable.Delete();
            }
        }

        private static void RemoveEntity(string tableName , TableEntity tableEntity)
        {
            if (string.IsNullOrEmpty(tableName) || tableEntity == null) return;

            var cloudTable = _azureTables.CloudTableClient.GetTableReference(tableName);
            if (cloudTable != null && cloudTable.Exists())
            {
                var deleteOperation = TableOperation.Delete(tableEntity);
                cloudTable.Execute(deleteOperation);
            }
        }

        private static TestTableEntity GetTableEntity(string tableName, string partitionKey, string rowKey)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(partitionKey) || string.IsNullOrEmpty(rowKey))
                return null;

            var cloudTable = _azureTables.CloudTableClient.GetTableReference(tableName);
            if (cloudTable == null || !cloudTable.Exists()) return null;

            var retrieveTableOperation = TableOperation.Retrieve<TestTableEntity>(partitionKey, rowKey);
            var result = cloudTable.Execute(retrieveTableOperation);
            return result?.Result as TestTableEntity;
        }
    }
}