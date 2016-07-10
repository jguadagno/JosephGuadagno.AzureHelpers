using System;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;

namespace JosephGuadagno.AzureHelpers.Storage
{
    public static class CloudStorageAccountHelper
    {
        /// <summary>
        /// Gets a reference to a storage account from a Url
        /// </summary>
        /// <param name="storageConnectionString">The Url to the storage account</param>
        /// <returns></returns>
        public static CloudStorageAccount GetStorageAccount(string storageConnectionString)
        {
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentNullException(storageConnectionString, "The storage connection string can not be null");
            }
            var configurationValue = CloudConfigurationManager.GetSetting(storageConnectionString);
            return configurationValue == null? null : CloudStorageAccount.Parse(configurationValue);
        }
    }
}