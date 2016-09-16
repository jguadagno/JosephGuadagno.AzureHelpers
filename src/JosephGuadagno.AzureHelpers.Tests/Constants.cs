using System.Configuration;

namespace JosephGuadagno.AzureHelpers.Tests
{
    public static class Constants
    {
        public static readonly string ExistingValidAppSettingsTableStorageConfigurationName =
            "TableStorageConnectionString";
        public static readonly string NonExistingAppSettingsTableStorageConfigurationName =
            "TableStorageConnectionString_DoesNotExist";
        public static readonly string ExistingInvalidBadAppSettingsTableStorageConfigurationName =
            "BadTableStorageConnectionString";

        public static readonly string ExistingValidAppSettingsTableStorageConnectionString =
            ConfigurationManager.AppSettings[ExistingValidAppSettingsTableStorageConfigurationName];
        public static readonly string NonExistingAppSettingsTableStorageConnectionString =
            ConfigurationManager.AppSettings[NonExistingAppSettingsTableStorageConfigurationName];
        public static readonly string ExistingInvalidBadAppSettingsTableStorageConnectionString =
            ConfigurationManager.AppSettings[ExistingInvalidBadAppSettingsTableStorageConfigurationName];
    }
}