using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace JosephGuadagno.AzureHelpers.Storage
{
	public static class ByteArraySerializer<T>
	{
		public static byte[] Serialize(T m)
		{
			var memoryStream = new MemoryStream();
			try
			{
				var formatter = new BinaryFormatter();
				formatter.Serialize(memoryStream, m);
				return memoryStream.ToArray();
			}
			finally
			{
				memoryStream.Close();
			}
		}

		public static T Deserialize(byte[] byteArray)
		{
			var memoryStream = new MemoryStream(byteArray);
			try
			{
				var formatter = new BinaryFormatter();
				return (T)formatter.Deserialize(memoryStream);
			}
			finally
			{
				memoryStream.Close();
			}
		}
	}
}
