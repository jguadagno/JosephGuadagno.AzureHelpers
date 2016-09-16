using JosephGuadagno.AzureHelpers.Compute.ServiceBus;
using JosephGuadagno.AzureHelpers.Tests.Models;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace JosephGuadagno.AzureHelpers.Tests.Compute.ServiceBus
{

	[TestClass]
	public class ServiceBusHelperTests
	{
		private const string TestSubscriptionName = "TestSubscription";
		private const string TestTopicName = "TestTopic";
		private readonly string _connectionString = Constants.ExistingValidAppSettingsTableStorageConnectionString;
		private NamespaceManager _namespaceManager;

		[TestInitialize]
		public void TestInitialize()
		{
			_namespaceManager = NamespaceManager.CreateFromConnectionString(_connectionString);
		}

		[TestCleanup]
		public void TestCleanup()
		{
            // Make sure no subscription exists
			_namespaceManager.DeleteSubscription(TestTopicName, TestSubscriptionName);
			// Remove the topic
			_namespaceManager.DeleteTopic(TestTopicName);

		}

		private static Email CreateTestMessage()
		{
			return new Email
			{
				Body = "Test Body",
				FromMailAddress = "jguadagno@hotmail.com",
				FromDisplayName = "Joseph Guadagno",
				ToMailAddress = "jguadagno@gmail.com",
				ToDisplayName = "Joseph Guadagno (Test)",
				Subject = "Test Email"
			};
		}

		[TestMethod]
		[Ignore]
		public void CreateTopic()
		{
			// Arrange
			var serviceBusHelper = new ServiceBusHelper(_connectionString);

			// Act
			var topic = serviceBusHelper.CreateTopic(TestTopicName);

			// Asset
			Assert.IsNotNull(topic);
			Assert.IsTrue(_namespaceManager.TopicExists(TestTopicName));
			_namespaceManager.DeleteTopic(TestTopicName);
		}

		[TestMethod]
		[Ignore]
		public void SubscribeToTopic()
		{
			// Arrange
			var serviceBusHelper = new ServiceBusHelper(_connectionString);
			_namespaceManager.CreateTopic(TestTopicName);

			// Act 
			serviceBusHelper.SubscribeToTopic(TestTopicName, TestSubscriptionName);

			// Assert
			Assert.IsTrue(_namespaceManager.SubscriptionExists(TestTopicName,TestSubscriptionName));

			// Cleanup
			_namespaceManager.DeleteSubscription(TestTopicName,TestSubscriptionName);
			_namespaceManager.DeleteTopic(TestTopicName);
		}

		[TestMethod]
		[Ignore]
		public void UnsubscribeFromTopic()
		{
			// Arrange
			var serviceBusHelper = new ServiceBusHelper(_connectionString);
			_namespaceManager.CreateTopic(TestTopicName);
			_namespaceManager.CreateSubscription(TestTopicName, TestSubscriptionName);

			// Act
			serviceBusHelper.UnsubscribeFromTopic(TestTopicName,TestSubscriptionName);

			// Assert
			Assert.IsFalse(_namespaceManager.SubscriptionExists(TestTopicName, TestSubscriptionName));
			
			// Cleanup
			_namespaceManager.DeleteTopic(TestTopicName);
		}

		[TestMethod]
		[Ignore]
		public void SendMessage()
		{
			// Arrange
			var serviceBusHelper = new ServiceBusHelper(_connectionString);
			_namespaceManager.CreateTopic(TestTopicName);
			_namespaceManager.CreateSubscription(TestTopicName, TestSubscriptionName);
			var emailMessage = CreateTestMessage();
			
			// Act
			serviceBusHelper.SendMessage(TestTopicName, new BrokeredMessage(emailMessage));
			
			// Assert
			var client = SubscriptionClient.CreateFromConnectionString(_connectionString, TestTopicName, TestSubscriptionName);
			
			var message = client.Receive();
			message.Complete();

			Assert.IsNotNull(message);
			Assert.IsTrue(ObjectHelper.AreEqual(emailMessage,message.GetBody<Email>()));

			// Cleanup
			_namespaceManager.DeleteSubscription(TestTopicName, TestSubscriptionName);
			_namespaceManager.DeleteTopic(TestTopicName);
		}

		[TestMethod]
		[Ignore]
		public void GetSubscriptionClient()
		{
			// Arrange
			var serviceBusHelper = new ServiceBusHelper(_connectionString);
			_namespaceManager.CreateTopic(TestTopicName);
			_namespaceManager.CreateSubscription(TestTopicName, TestSubscriptionName);

			// Act
			var client =serviceBusHelper.GetSubscriptionClient(TestTopicName, TestSubscriptionName);

			// Assert
			Assert.IsNotNull(client);
			Assert.AreEqual(TestSubscriptionName, client.Name);
			Assert.AreEqual(TestTopicName, client.TopicPath);

			// Cleanup
			_namespaceManager.DeleteSubscription(TestTopicName, TestSubscriptionName);
			_namespaceManager.DeleteTopic(TestTopicName);
		}
	}
}