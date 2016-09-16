using JosephGuadagno.AzureHelpers.Extensions;
using JosephGuadagno.AzureHelpers.Storage;
using JosephGuadagno.AzureHelpers.Tests.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace JosephGuadagno.AzureHelpers.Tests.Storage
{
	[TestClass]
	public class QueueHelperTests
	{

		private const string TestQueueName = "testqueue";
		private static CloudStorageAccount _cloudStorageAccount;
		private static CloudQueueClient _cloudQueueClient;


		[ClassInitialize]
		public static void ClassInitialize(TestContext testContext)
		{
			_cloudStorageAccount = CloudStorageAccount.Parse(Constants.ExistingValidAppSettingsTableStorageConnectionString);
			_cloudQueueClient = _cloudStorageAccount.CreateCloudQueueClient();
		}

		[ClassCleanup]
		public static void ClassCleanup()
		{
			var queue = _cloudQueueClient.GetQueueReference(TestQueueName);
			queue.DeleteIfExists();
		}

		private Email GetTestMessageObject()
		{
			return new Email
			{
				Body = "Body",
				Subject = "Subject",
				FromDisplayName = "Joseph Guadagno",
				FromMailAddress = "jguadagno@hotmail.com",
				ToDisplayName = "Test User",
				ToMailAddress = "jguadagno@gmail.com"
			};
		}

		[TestMethod]
		public void ConstructorTest_Empty()
		{
			// Arrange

			// Act
			var queueHelper = new Queue();

			// Assert
			Assert.IsNotNull(queueHelper);
			Assert.IsNotNull(queueHelper.CloudStorageAccount);
			Assert.IsNotNull(queueHelper.CloudStorageAccount.QueueStorageUri);

		}

		[TestMethod]
		public void ConstructorTest_WithStorageAccount()
		{
			// Arrange

			// Act
			var queueHelper = new Queue(_cloudStorageAccount);

			// Assert
			Assert.IsNotNull(queueHelper);
			Assert.IsNotNull(queueHelper.CloudStorageAccount);
			Assert.IsNotNull(queueHelper.CloudStorageAccount.QueueStorageUri);
		}

		[TestMethod]
		public void ConstructorTest_WithStorageAccountUrl()
		{
			// Arrange

			// Act
			var queueHelper = new Queue(Constants.ExistingValidAppSettingsTableStorageConfigurationName);

			// Assert
			Assert.IsNotNull(queueHelper);
			Assert.IsNotNull(queueHelper.CloudStorageAccount);
			Assert.IsNotNull(queueHelper.CloudStorageAccount.QueueStorageUri);
		}

		[TestMethod]
		public void GetStorageAccountTest()
		{
			// Arrange

			// Act
			var storageAccount = AzureHelpers.Storage.CloudStorageAccountHelper.GetStorageAccount(Constants.ExistingValidAppSettingsTableStorageConfigurationName);

			// Assert
			Assert.IsNotNull(storageAccount);
			Assert.IsNotNull(storageAccount.QueueStorageUri);
		}

		[TestMethod]
		public void GetStorageAccountFromConfigurationFileTest_WithConfigurationSetting()
		{
			// Arrange

			// Act
			var storageAccount = AzureHelpers.Storage.CloudStorageAccountHelper.GetStorageAccount(Constants.ExistingValidAppSettingsTableStorageConfigurationName);

			// Assert
			Assert.IsNotNull(storageAccount);
			Assert.IsNotNull(storageAccount.QueueStorageUri);
		}

		[TestMethod]
		public void GetQueueTest_WithQueueName()
		{
			// Arrange
			var queueHelper = new Queue(Constants.ExistingValidAppSettingsTableStorageConfigurationName);

			// Act
			var queue = queueHelper.GetQueue(TestQueueName);

			// Assert
			Assert.IsNotNull(queue);
			Assert.AreEqual(TestQueueName, queue.Name);
			var queueClient = _cloudStorageAccount.CreateCloudQueueClient();
			Assert.IsNotNull(queueClient.GetQueueReference(TestQueueName));

		}

		[TestMethod]
		public void CreateCloudQueueTest_WithQueueName()
		{
			// Arrange
			var queueHelper = new Queue(Constants.ExistingValidAppSettingsTableStorageConfigurationName);

			// Act
			var queue = queueHelper.CreateCloudQueue(TestQueueName);

			// Assert
			Assert.IsNotNull(queue);
			Assert.AreEqual(TestQueueName, queue.Name);
			var queueClient = _cloudStorageAccount.CreateCloudQueueClient();
			Assert.IsNotNull(queueClient.GetQueueReference(TestQueueName));

		}

		[TestMethod]
		public void AddMessageToQueueTest_WithQueueName()
		{
			// Arrange
			var queueHelper = new Queue(Constants.ExistingValidAppSettingsTableStorageConfigurationName);
			var testMessageObject = GetTestMessageObject();

			// Act
			queueHelper.AddMessageToQueue(TestQueueName, testMessageObject);

			// Assert
			var queueClient = _cloudStorageAccount.CreateCloudQueueClient();
			var queue = queueClient.GetQueueReference(TestQueueName);
			var message = queue.GetMessage();
			var queueObject = ByteArraySerializer<Email>.Deserialize(message.AsBytes);
			Assert.IsNotNull(queueObject);
			Assert.IsTrue(ObjectHelper.AreEqual(testMessageObject, queueObject));

		}

		[TestMethod]
		public void AddMessageToQueueTest_WithCloudQueue()
		{
			// Arrange
			var queueHelper = new Queue(Constants.ExistingValidAppSettingsTableStorageConfigurationName);
			var testMessageObject = GetTestMessageObject();
			var queue = queueHelper.GetQueue(TestQueueName);

			// Act
			queueHelper.AddMessageToQueue(queue, testMessageObject);

			// Assert
			var message = queue.GetMessage();
			var queueObject = ByteArraySerializer<Email>.Deserialize(message.AsBytes);
			Assert.IsNotNull(queueObject);
			Assert.IsTrue(ObjectHelper.AreEqual(testMessageObject, queueObject));
		}

		[TestMethod]
		public void GetMessageTest_WithQueueName()
		{
			// Arrange
			var queueClient = _cloudStorageAccount.CreateCloudQueueClient();
			var queue = queueClient.GetQueueReference(TestQueueName);
			queue.SafeCreateIfNotExists();
			var testMessageObject = GetTestMessageObject();
			queue.AddMessage(new CloudQueueMessage(ByteArraySerializer<Email>.Serialize(testMessageObject)));

			var queueHelper = new Queue(Constants.ExistingValidAppSettingsTableStorageConfigurationName);

			// Act
			var queueMessage = queueHelper.GetMessage<Email>(TestQueueName);

			// Assert
			Assert.IsNotNull(queueMessage);
			Assert.IsTrue(ObjectHelper.AreEqual(testMessageObject, queueMessage));

		}

		[TestMethod]
		public void GetMessageTest_WithCloudQueue()
		{
			// Arrange
			var queueClient = _cloudStorageAccount.CreateCloudQueueClient();
			var queue = queueClient.GetQueueReference(TestQueueName);
			queue.SafeCreateIfNotExists();
			var testMessageObject = GetTestMessageObject();
			queue.AddMessage(new CloudQueueMessage(ByteArraySerializer<Email>.Serialize(testMessageObject)));

			var queueHelper = new Queue(Constants.ExistingValidAppSettingsTableStorageConnectionString);

			// Act
			var queueMessage = queueHelper.GetMessage<Email>(queue);

			// Assert
			Assert.IsNotNull(queueMessage);
			Assert.IsTrue(ObjectHelper.AreEqual(testMessageObject, queueMessage));
		}
	}
}