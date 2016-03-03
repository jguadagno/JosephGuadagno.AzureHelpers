using System.Threading;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob.Protocol;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Queue.Protocol;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Protocol;

namespace JosephGuadagno.AzureHelpers.Extensions
{
	public static class Storage
	{
		public static bool SafeCreateIfNotExists(this CloudTable table, TableRequestOptions requestOptions = null,
			OperationContext operationContext = null)
		{
			do
			{
				try
				{
					return table.CreateIfNotExists(requestOptions, operationContext);
				}
				catch (StorageException e)
				{
					if ((e.RequestInformation.HttpStatusCode == 409) &&
					    (e.RequestInformation.ExtendedErrorInformation.ErrorCode.Equals(TableErrorCodeStrings.TableBeingDeleted)))
						Thread.Sleep(1000); // The table is currently being deleted. Try again until it works.
					else
						throw;
				}
			} while (true);
		}

		public static bool SafeCreateIfNotExists(this CloudBlobContainer blobContainer,
			BlobRequestOptions blobRequestOptions = null, OperationContext operationContext = null)
		{
			do
			{
				try
				{
					return blobContainer.CreateIfNotExists(blobRequestOptions, operationContext);
				}
				catch (StorageException e)
				{
					if ((e.RequestInformation.HttpStatusCode == 409) &&
					    (e.RequestInformation.ExtendedErrorInformation.ErrorCode.Equals(BlobErrorCodeStrings.ContainerBeingDeleted)))
						Thread.Sleep(1000); // The table is currently being deleted. Try again until it works.
					else
						throw;
				}
			} while (true);
		}

		public static bool SafeCreateIfNotExists(this CloudQueue cloudQueue, QueueRequestOptions requestOptions = null,
			OperationContext operationContext = null)
		{
			do
			{
				try
				{
					return cloudQueue.CreateIfNotExists(requestOptions, operationContext);
				}
				catch (StorageException e)
				{
					if ((e.RequestInformation.HttpStatusCode == 409) &&
					    (e.RequestInformation.ExtendedErrorInformation.ErrorCode.Equals(QueueErrorCodeStrings.QueueBeingDeleted)))
						Thread.Sleep(1000); // The table is currently being deleted. Try again until it works.
					else
						throw;
				}
			} while (true);
		}
	}
}