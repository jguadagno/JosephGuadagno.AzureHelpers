using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Net;
using JosephGuadagno.AzureHelpers.Extensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace JosephGuadagno.AzureHelpers.Storage
{
	/// <summary>
	/// Provides functions to interact with Blob Storage
	/// </summary>
	public static class Blob
	{
		private static readonly Dictionary<string, CloudBlobContainer> BlobContainers = new Dictionary<string, CloudBlobContainer>();
		private static readonly object Lock = new object();

		/// <summary>
		/// Gets a reference to a blob container
		/// </summary>
		/// <param name="containerName">The name of the container</param>
		/// <param name="configurationName">The configuration key name for the Windows Azure blob storage account</param>
		/// <returns>The container reference</returns>
		public static CloudBlobContainer GetBlobContainer(string containerName, string configurationName = "AzureBlobStorageConnectionString")
		{
			lock (Lock)
			{
				if (BlobContainers.ContainsKey(containerName))
				{
					return BlobContainers[containerName];
				}
			}
			CreateCloudBlobClient(containerName);
			lock (Lock)
			{
				return BlobContainers[containerName];
			}
		}

		/// <summary>
		/// Creates the blob container
		/// </summary>
		/// <param name="containerName">The container name</param>
		/// <param name="configurationName">The configuration key name for the Windows Azure blob storage account</param>
		/// <returns>The newly created blob container</returns>
		public static CloudBlobContainer CreateCloudBlobClient(string containerName, string configurationName = "AzureBlobStorageConnectionString")
		{
			lock (Lock)
			{
				try
				{
					if (BlobContainers.ContainsKey(containerName))
					{
						return BlobContainers[containerName];
					}

                    // TODO: Fix Microsoft.WindowsAzure (RoleEnvironment)
					//string configurationSetting = RoleEnvironment.IsAvailable
					//	? RoleEnvironment.GetConfigurationSettingValue(configurationName)
					//	: ConfigurationManager.AppSettings[configurationName];
				    var configurationSetting = "";

					CloudStorageAccount storageAccount = CloudStorageAccount.Parse(configurationSetting);
					CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
					CloudBlobContainer blobContainer = blobClient.GetContainerReference(containerName);

					blobContainer.SafeCreateIfNotExists();
					var permissions = blobContainer.GetPermissions();
					permissions.PublicAccess = BlobContainerPublicAccessType.Container;
					blobContainer.SetPermissions(permissions);

					BlobContainers.Add(containerName, blobContainer);
					return blobContainer;
				}
				catch (WebException)
				{

					throw new WebException("The Windows Azure storage services cannot be contacted " +
						 "via the current account configuration or the local development storage tool is not running. " +
						 "Please start the development storage tool if you run the service locally!");
				}
			}
		}

		/// <summary>
		/// Stored a file and stream to an Windows Azure Blob container.
		/// </summary>
		/// <param name="containerName">The name of the container to store the file.</param>
		/// <param name="fileStream">The stream to store</param>
		/// <param name="fileName">The name of the file</param>
		/// <param name="contentType">The content type of the file</param>
		/// <returns></returns>
		public static string SendFileToBlob(string containerName, Stream fileStream, string fileName, string contentType)
		{
			var container = GetBlobContainer(containerName);

			CloudBlockBlob blockBlob = container.GetBlockBlobReference(fileName);
			blockBlob.Properties.ContentType = contentType;
			blockBlob.UploadFromStream(fileStream);

			return fileName;
		}

		/// <summary>
		/// Get a stream from the Windows Azure blob
		/// </summary>
		/// <param name="containerName">The container that the file is in</param>
		/// <param name="blobName">The name of the blob to get.</param>
		/// <returns>The contents of the blob</returns>
		public static Stream GetStream(string containerName, string blobName)
		{
			var container = GetBlobContainer(containerName);
			CloudBlockBlob content = container.GetBlockBlobReference(blobName);

			MemoryStream memoryStream = new MemoryStream();
			content.DownloadToStream(memoryStream);
			return memoryStream;
		}

		/// <summary>
		/// Gets the Url of the blob
		/// </summary>
		/// <param name="containerName">The container the blob is stored on</param>
		/// <param name="blobName">The name of the blob</param>
		/// <returns>The Url of the blob if found, otherwise null</returns>
		public static string GetBlobUrl(string containerName, string blobName)
		{
			if (string.IsNullOrEmpty(containerName) || string.IsNullOrEmpty(blobName))
			{
				return null;
			}

			try
			{
				var container = GetBlobContainer(containerName);
				var blob = container.GetBlobReferenceFromServer(blobName);
				return blob.Uri.ToString();
			}
			catch (Exception ex)
			{
				Trace.TraceError("An exception occurred when trying to Getting Blob Uri. Container:{0}, blobName:{1}, Exception:{2}", containerName, blobName, ex.ToString());
				return null;
			}
			
		}

		/// <summary>
		/// Helps to generate a unique filename
		/// </summary>
		/// <param name="filename"></param>
		/// <returns></returns>
		public static string GenerateUniqueFilename(string filename)
		{
			return $"{filename}_{Guid.NewGuid()}_{DateTime.Now.Ticks:10}";
		}
	}
}
