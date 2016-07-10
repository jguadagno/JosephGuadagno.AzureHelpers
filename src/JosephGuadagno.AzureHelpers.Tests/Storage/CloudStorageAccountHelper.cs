using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace JosephGuadagno.AzureHelpers.Tests.Storage
{
    [TestClass]
    public class CloudStorageAccountHelper
    {
        [TestMethod]
        public void GetCloudAccountWithExistingValidAppSettings()
        {
            var cloudAccount =
                AzureHelpers.Storage.CloudStorageAccountHelper.GetStorageAccount(
                    Constants.ExistingValidAppSettingsTableStorageConnectionString);
            Assert.IsNotNull(cloudAccount);
        }

        [TestMethod]
        [ExpectedException(typeof(FormatException))]
        public void GetCloudAccountWithExistingInvalidAppSettings()
        {
            var cloudAccount =
                AzureHelpers.Storage.CloudStorageAccountHelper.GetStorageAccount(
                    Constants.ExistingInvalidBadAppSettingsTableStorageConnectionString);
            Assert.IsNull(cloudAccount);
        }

        [TestMethod]
        [ExpectedException(typeof(FormatException))]
        public void GetCloudAccountWithNonExistingAppSettings()
        {
            var cloudAccount =
                AzureHelpers.Storage.CloudStorageAccountHelper.GetStorageAccount(
                    Constants.ExistingInvalidBadAppSettingsTableStorageConnectionString);
            Assert.IsNull(cloudAccount);
        }

        [TestMethod]
        [ExpectedException(typeof(ArgumentNullException))]
        public void GetCloudAccountWithNullConnectionString()
        {
            var cloudAccount = AzureHelpers.Storage.CloudStorageAccountHelper.GetStorageAccount(null);
        }
    }
}
