using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.Azure;

namespace JosephGuadagno.AzureHelpers.Compute.ServiceBus
{

	public class ServiceBusHelper
	{

		private readonly string _connectionString;
		private readonly NamespaceManager _namespaceManager;

		public ServiceBusHelper()
		{
			_connectionString = CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
			_namespaceManager = GetNamespaceManager(_connectionString);
		}

		public ServiceBusHelper(string connectionString)
		{
			_connectionString = connectionString;
			_namespaceManager = GetNamespaceManager(connectionString);
		}

		private static NamespaceManager GetNamespaceManager(string connectionString)
		{
			return NamespaceManager.CreateFromConnectionString(connectionString);
		}

		public static TopicDescription CreateTopic(string connectionString, string topicName)
		{
			var namespaceManager = GetNamespaceManager(connectionString);
			return namespaceManager.TopicExists(topicName)
				? namespaceManager.GetTopic(topicName)
				: namespaceManager.CreateTopic(topicName);
		}

		public TopicDescription CreateTopic(string topicName)
		{
			return _namespaceManager.TopicExists(topicName)
				? _namespaceManager.GetTopic(topicName)
				: _namespaceManager.CreateTopic(topicName);
		}

		public void SubscribeToTopic(string topicName, string subscriptionName)
		{
			if (!_namespaceManager.SubscriptionExists(topicName, subscriptionName))
			{
				_namespaceManager.CreateSubscription(topicName, subscriptionName);
			}
		}

		public void SubscribeToTopic(string topicName, string subscriptionName, string filter)
		{
			SubscribeToTopic(topicName, subscriptionName, new SqlFilter(filter));
		}

		public void SubscribeToTopic(string topicName, string subscriptionName, SqlFilter sqlFilter)
		{
			if (!_namespaceManager.SubscriptionExists(topicName, subscriptionName))
			{
				_namespaceManager.CreateSubscription(topicName, subscriptionName, sqlFilter);
			}
		}

		public void UnsubscribeFromTopic(string topicName, string subscriptionName)
		{
			_namespaceManager.DeleteSubscription(topicName, subscriptionName);
		}

		public void SendMessage(string topicName, BrokeredMessage brokeredMessage)
		{
			SendMessage(_connectionString, topicName, brokeredMessage);
		}

		public static void SendMessage(string connectionString, string topicName, BrokeredMessage brokeredMessage)
		{
			var topicClient = TopicClient.CreateFromConnectionString(connectionString, topicName);
			topicClient.Send(brokeredMessage);
		}

		public SubscriptionClient GetSubscriptionClient(string topicName, string subscriptionName)
		{
			return GetSubscriptionClient(_connectionString, topicName, subscriptionName);
		}

		public static SubscriptionClient GetSubscriptionClient(string connectionString, string topicName,
			string subscriptionName)
		{
			return SubscriptionClient.CreateFromConnectionString(connectionString, topicName, subscriptionName);
		}
	}
}