using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using JosephGuadagno.AzureHelpers.Extensions;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace JosephGuadagno.AzureHelpers.Storage
{
    public class AzureTables
    {
        /// <summary>
		/// The default app setting key name to use for the storage account url
		/// </summary>
		public const string DefaultConfigurationKeyName = "AzureStorageConnectionString_Table";

        private readonly object _lock = new object();
        private CloudStorageAccount _cloudStorageAccount;

        /// <summary>
        /// Creates an instance of the <see cref="AzureTables"/> using the default configuration key from configuration file.
        /// </summary>
        public AzureTables()
        {
            CloudStorageAccount = CloudStorageAccountHelper.GetStorageAccount(DefaultConfigurationKeyName);
            
        }

        /// <summary>
        /// Creates an instance of the <see cref="AzureTables"/> using the supplied <paramref name="storageConnectionString">Storage Account Url</paramref>
        /// </summary>
        /// <param name="storageConnectionString">A url to the cloud storage account to use.</param>
        public AzureTables(string storageConnectionString)
        {
            CloudStorageAccount = CloudStorageAccountHelper.GetStorageAccount(storageConnectionString);
        }

        /// <summary>
        /// Creates an instance of the <see cref="AzureTables"/> using the supplied <paramref name="cloudStorageAccount">CloudStorageAccount</paramref>
        /// </summary>
        /// <param name="cloudStorageAccount"></param>
        public AzureTables(CloudStorageAccount cloudStorageAccount)
        {
            CloudStorageAccount = cloudStorageAccount;
        }

        public Dictionary<string, CloudTable> CloudTables { get; } = new Dictionary<string, CloudTable>();

        public CloudStorageAccount CloudStorageAccount
        {
            get { return _cloudStorageAccount; }
            set
            {
                if (value == null) return;
                _cloudStorageAccount = value;
                CloudTableClient = _cloudStorageAccount.CreateCloudTableClient();
            }
        }

        public CloudTableClient CloudTableClient { get; set; }

        
        public CloudTable GetCloudTable(string tableName, bool createIfNeeded = true)
        {

            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }

            lock (_lock)
            {
                if (CloudTables.ContainsKey(tableName))
                {
                    return CloudTables[tableName];
                }
            }

            var cloudTable = CloudTableClient.GetTableReference(tableName);

            if (cloudTable.Exists() == false && createIfNeeded == false)
            {
                return null;
            }

            CreateCloudTable(tableName);
            lock (_lock)
            {
                return CloudTables[tableName];
            }
        }

        public bool DoesCloudTableExists(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name is required");
            }
            return GetCloudTable(tableName, false) != null;
        }

        public CloudTable CreateCloudTable(string tableName)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }

            var cloudTable = CloudTableClient.GetTableReference(tableName);
            return CreateCloudTable(cloudTable);

        }

        public CloudTable CreateCloudTable(CloudTable cloudTable)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The table can not be null");
            }

            lock (_lock)
            {
                try
                {
                    cloudTable.SafeCreateIfNotExists();
                    CloudTables.Add(cloudTable.Name, cloudTable);

                    return cloudTable;
                }
                catch (WebException)
                {

                    throw new WebException("The Windows Azure storage services cannot be contacted " +
                         "via the current account configuration or the local development storage tool is not running. " +
                         "Please start the development storage tool if you run the service locally!");
                }
            }
        }

        public void DeleteCloudTable(string tableName)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }

            var cloudTable = GetCloudTable(tableName);
            DeleteCloudTable(cloudTable);
        }

        public void DeleteCloudTable(CloudTable cloudTable)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The table name can not be null");
            }

            if (cloudTable.Exists())
            {
                cloudTable.Delete();
            }
        }

        public HttpStatusCode InsertEntity(string tableName, TableEntity tableEntity)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The entity was null");
            }

            var cloudTable = GetCloudTable(tableName, false);
            if (cloudTable == null)
            {
                throw new NullReferenceException($"Could not find a table with the name {tableName}");
            }

            return InsertEntity(cloudTable, tableEntity);
        }

        public HttpStatusCode InsertEntity(CloudTable cloudTable, TableEntity tableEntity)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The cloud table specified is null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The table entity is null");
            }

            var insertTableOperation = TableOperation.Insert(tableEntity);
            var tableResult = cloudTable.Execute(insertTableOperation);

            return (HttpStatusCode) tableResult.HttpStatusCode;
        }

        public HttpStatusCode InsertOrMergeEntity(string tableName, TableEntity tableEntity)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The entity was null");
            }

            var cloudTable = GetCloudTable(tableName, false);
            if (cloudTable == null)
            {
                throw new NullReferenceException($"Could not find a table with the name {tableName}");
            }

            return InsertOrMergeEntity(cloudTable, tableEntity);
        }

        public HttpStatusCode InsertOrMergeEntity(CloudTable cloudTable, TableEntity tableEntity)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The cloud table specified is null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The table entity is null");
            }

            var insertOrMergeOperation = TableOperation.InsertOrMerge(tableEntity);
            var tableResult = cloudTable.Execute(insertOrMergeOperation);

            return (HttpStatusCode)tableResult.HttpStatusCode;
        }

        public HttpStatusCode InsertOrReplaceEntity(string tableName, TableEntity tableEntity)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The entity was null");
            }

            var cloudTable = GetCloudTable(tableName, false);
            if (cloudTable == null)
            {
                throw new NullReferenceException($"Could not find a table with the name {tableName}");
            }

            return InsertOrReplaceEntity(cloudTable, tableEntity);
        }

        public HttpStatusCode InsertOrReplaceEntity(CloudTable cloudTable, TableEntity tableEntity)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The cloud table specified is null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The table entity is null");
            }

            var insertOrReplaceOperation = TableOperation.InsertOrReplace(tableEntity);
            var tableResult = cloudTable.Execute(insertOrReplaceOperation);

            return (HttpStatusCode)tableResult.HttpStatusCode;
        }
        public HttpStatusCode MergeEntity(string tableName, TableEntity tableEntity)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The entity was null");
            }

            var cloudTable = GetCloudTable(tableName, false);
            if (cloudTable == null)
            {
                throw new NullReferenceException($"Could not find a table with the name {tableName}");
            }

            return MergeEntity(cloudTable, tableEntity);
        }

        public HttpStatusCode MergeEntity(CloudTable cloudTable, TableEntity tableEntity)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The cloud table specified is null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The table entity is null");
            }

            var mergeOperation = TableOperation.Insert(tableEntity);
            var tableResult = cloudTable.Execute(mergeOperation);

            return (HttpStatusCode)tableResult.HttpStatusCode;
        }

        public HttpStatusCode ReplaceEntity(string tableName, TableEntity tableEntity)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The entity was null");
            }

            var cloudTable = GetCloudTable(tableName, false);
            if (cloudTable == null)
            {
                throw new NullReferenceException($"Could not find a table with the name {tableName}");
            }

            return ReplaceEntity(cloudTable, tableEntity);
        }

        public HttpStatusCode ReplaceEntity(CloudTable cloudTable, TableEntity tableEntity)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The cloud table specified is null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The table entity is null");
            }

            var replaceTableOperation = TableOperation.Insert(tableEntity);
            var tableResult = cloudTable.Execute(replaceTableOperation);

            return (HttpStatusCode)tableResult.HttpStatusCode;
        }

        public HttpStatusCode DeleteEntity(string tableName, TableEntity tableEntity)
        {
            if (CloudStorageAccount == null)
            {
                throw new NullReferenceException("The CloudStorageAccount is null");
            }
            if (string.IsNullOrEmpty(tableName))
            {
                throw new ArgumentNullException(tableName, "The table name can not be null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The entity was null");
            }

            var cloudTable = GetCloudTable(tableName, false);
            if (cloudTable == null)
            {
                throw new NullReferenceException($"Could not find a table with the name {tableName}");
            }

            return DeleteEntity(cloudTable, tableEntity);
        }

        public HttpStatusCode DeleteEntity(CloudTable cloudTable, TableEntity tableEntity)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException(nameof(cloudTable), "The cloud table specified is null");
            }
            if (tableEntity == null)
            {
                throw new ArgumentNullException(nameof(tableEntity), "The table entity is null");
            }

            var deleteTableOperation = TableOperation.Delete(tableEntity);
            var tableResult = cloudTable.Execute(deleteTableOperation);

            return (HttpStatusCode)tableResult.HttpStatusCode;
        }

        public T GetTableEntity<T>(string tableName, string partitionKey, string rowKey) where T : class, ITableEntity
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(partitionKey) || string.IsNullOrEmpty(rowKey))
                return null;

            var cloudTable = GetCloudTable(tableName, false);
            return GetTableEntity<T>(cloudTable, partitionKey, rowKey);
        }

        public T GetTableEntity<T>(CloudTable cloudTable, string partitionKey, string rowKey) where T : class, ITableEntity
        {
            if (cloudTable == null || string.IsNullOrEmpty(partitionKey) || string.IsNullOrEmpty(rowKey))
                return null;

            if (!cloudTable.Exists()) return null;

            var retrieveTableOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);
            var result = cloudTable.Execute(retrieveTableOperation);
            return result?.Result as T;
        }
    }
}