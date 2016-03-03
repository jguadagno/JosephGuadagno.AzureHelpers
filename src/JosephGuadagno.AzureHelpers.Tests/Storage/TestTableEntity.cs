using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace JosephGuadagno.AzureHelpers.Tests.Storage
{
    public class TestTableEntity: TableEntity
    {
        public TestTableEntity()
        {
        }

        public TestTableEntity(string partitionKey, string rowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
        }
        public string Property1 { get; set; }
        public string Property2 { get; set; }
    }
}
