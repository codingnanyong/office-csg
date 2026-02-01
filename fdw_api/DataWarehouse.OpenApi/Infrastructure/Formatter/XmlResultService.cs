using CSG.MI.FDW.LoggerService;
using System.Xml.Serialization;

namespace CSG.MI.FDW.OpenApi.Infrastructure.Formatter
{
	/// <summary>
	/// Implement generic data list XML download Service Class
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class XmlResultService<T> : IXmlResultService<T>
	{
		#region Constructors
		private readonly ILoggerManager _logger;

		public XmlResultService(ILoggerManager logger)
		{
			_logger = logger;
		}

		#endregion

		/// <summary>
		/// Implement generic data list XML download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		/// <exception cref="NotImplementedException"></exception>
		public MemoryStream Download(List<T> data, string fileName)
		{
			_logger.LogInfo("Execute the List<T> list XML download method");

			try
			{
				if (data == null || data.Count == 0)
					throw new InvalidOperationException("Cannot serialize a null or empty collection.");

				var xmlSerializer = new XmlSerializer(typeof(List<T>));
				var memoryStream = new MemoryStream();
				xmlSerializer.Serialize(memoryStream, data);

				memoryStream.Seek(0, SeekOrigin.Begin);

				_logger.LogInfo("File '{fileName}' downloaded successfully.", fileName);
				return memoryStream;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to download file '{fileName}'.", fileName);
				throw;
			}
		}

		/// <summary>
		/// Implement generic data list XML download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		/// <exception cref="NotImplementedException"></exception>
		public MemoryStream Download(T data, string fileName)
		{
			_logger.LogInfo("Execute the Generic T XML download method");

			try
			{
				var xmlSerializer = new XmlSerializer(typeof(T));
				var memoryStream = new MemoryStream();
				xmlSerializer.Serialize(memoryStream, data);

				memoryStream.Seek(0, SeekOrigin.Begin);

				_logger.LogInfo("File '{fileName}' downloaded successfully.", fileName);
				return memoryStream;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to download file '{fileName}'.", fileName);
				throw;
			}
		}
	}
}
