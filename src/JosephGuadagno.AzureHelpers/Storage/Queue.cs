using System.Collections.Generic;
using System.Net;
using JosephGuadagno.AzureHelpers.Extensions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace JosephGuadagno.AzureHelpers.Storage
{
	/// <summary>
	/// Provides functions to interact with Queues
	/// </summary>
	public class Queue
	{
		/// <summary>
		/// The default app setting key name to use for the storage account url
		/// </summary>
		public const string DefaultConfigurationKeyName = "AzureStorageConnectionString_Queue";

	    private readonly object _lock = new object();

		/// <summary>
		/// Creates an instance of the <see cref="Queue"/> using the default configuration key from configuration file.
		/// </summary>
		public Queue()
		{
			CloudStorageAccount = CloudStorageAccountHelper.GetStorageAccount(DefaultConfigurationKeyName);
		}

		/// <summary>
		/// Creates an instance of the <see cref="Queue"/> using the supplied <paramref name="storageConnectionString">Storage Account Url</paramref>
		/// </summary>
		/// <param name="storageConnectionString">A url to the cloud storage account to use.</param>
		public Queue(string storageConnectionString)
		{
			CloudStorageAccount = CloudStorageAccountHelper.GetStorageAccount(storageConnectionString);
		}

		/// <summary>
		/// Creates an instance of the <see cref="Queue"/> using the supplied <paramref name="cloudStorageAccount">CloudStorageAccount</paramref>
		/// </summary>
		/// <param name="cloudStorageAccount"></param>
		public Queue(CloudStorageAccount cloudStorageAccount)
		{
			CloudStorageAccount = cloudStorageAccount;
		}

		public Dictionary<string, CloudQueue> CloudQueues { get; } = new Dictionary<string, CloudQueue>();

	    public CloudStorageAccount CloudStorageAccount { get; set; }

		/// <summary>
		/// Gets a reference to a Queue
		/// </summary>
		public CloudQueue GetQueue(string queueName)
		{
			lock (_lock)
			{
				if (CloudQueues.ContainsKey(queueName))
				{
					return CloudQueues[queueName];
				}
			}
			CreateCloudQueue(queueName);
			lock (_lock)
			{
				return CloudQueues[queueName];
			}
		}

		/// <summary>
		/// Creates a Queue
		/// </summary>
		/// <param name="queueName">The name of the Queue</param>
		/// <returns></returns>
		public CloudQueue CreateCloudQueue(string queueName)
		{
			lock (_lock)
			{
				try
				{
					if (CloudQueues.ContainsKey(queueName))
					{
						return CloudQueues[queueName];
					}
					
					CloudQueueClient queueClient = CloudStorageAccount.CreateCloudQueueClient();
					CloudQueue queue = queueClient.GetQueueReference(queueName);
					queue.SafeCreateIfNotExists();
					
					CloudQueues.Add(queueName, queue);
					return queue;

				}
				catch (WebException)
				{

					throw new WebException("he Windows Azure storage services cannot be contacted " +
						 "via the current account configuration or the local development storage tool is not running. " +
						 "Please start the development storage tool if you run the service locally!");
				}
			}
		}

		/// <summary>
		/// Adds a message to the requested queue
		/// </summary>
		/// <param name="queueName"></param>
		/// <param name="message"></param>
		/// <remarks>See: http://msdn.microsoft.com/en-us/library/microsoft.windowsazure.storage.queue.cloudqueuemessage_members.aspx </remarks>
		public void AddMessageToQueue<T>(string queueName, T message)
		{
			var queue = GetQueue(queueName);
			AddMessageToQueue(queue, message);
		}

		/// <summary>
		/// Adds a message to the requested queue
		/// </summary>
		/// <param name="cloudQueue"></param>
		/// <param name="message"></param>
		/// <remarks>See: http://msdn.microsoft.com/en-us/library/microsoft.windowsazure.storage.queue.cloudqueuemessage_members.aspx </remarks>
		public void AddMessageToQueue<T>(CloudQueue cloudQueue, T message)
		{
			cloudQueue.AddMessage(new CloudQueueMessage(ByteArraySerializer<T>.Serialize(message)));
		}

		/// <summary>
		/// Gets a message from the queue
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="queueName">The queue name to retrieve the message from</param>
		/// <returns></returns>
		public T GetMessage<T>(string queueName)
		{
			var queue = GetQueue(queueName);
			return GetMessage<T>(queue);
		}

		/// <summary>
		/// Gets a message from the queue
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="cloudQueue">The CloudQueue to retrieve the message from</param>
		/// <returns></returns>
		public T GetMessage<T>(CloudQueue cloudQueue)
		{
			var cloudQueueMessage = cloudQueue.GetMessage();
			return cloudQueueMessage == null ? default(T) : ByteArraySerializer<T>.Deserialize(cloudQueueMessage.AsBytes);
		}
	}
}