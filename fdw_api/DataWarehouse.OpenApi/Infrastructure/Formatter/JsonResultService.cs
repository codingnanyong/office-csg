using CSG.MI.FDW.LoggerService;
using Newtonsoft.Json;
using System.Text;

namespace CSG.MI.FDW.OpenApi.Infrastructure.Formatter
{
	/// <summary>
	/// Implement generic data list JSON download Service Class
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class JsonResultService<T> : IJsonResultService<T>
	{
		#region Constructors

		private readonly ILoggerManager _logger;

		public JsonResultService(ILoggerManager logger)
		{
			_logger = logger;
		}

		#endregion

		/// <summary>
		/// Implement generic data list JSON download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		public MemoryStream Download(ICollection<T> data, string fileName)
		{
			_logger.LogInfo("Execute the ICollection<T> list JSON download method");

			try
			{
				var json = JsonConvert.SerializeObject(data, Formatting.Indented);

				var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
				stream.Position = 0;

				_logger.LogInfo("File '{fileName}' downloaded successfully.", fileName);
				return stream;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to download file '{fileName}'.", fileName);
				throw;
			}
		}

		/// <summary>
		/// Implement generic data JSON download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		public MemoryStream Download(T data, string fileName)
		{
			_logger.LogInfo("Execute the Generic T JSON download method");

			try
			{
				var json = JsonConvert.SerializeObject(data, Formatting.Indented);

				var stream = new MemoryStream(Encoding.UTF8.GetBytes(json));
				stream.Position = 0;

				_logger.LogInfo("File '{fileName}' downloaded successfully.", fileName);
				return stream;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to download file '{fileName}'.", fileName);
				throw;
			}
		}
	}
}
