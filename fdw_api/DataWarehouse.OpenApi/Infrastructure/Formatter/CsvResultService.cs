using CSG.MI.FDW.LoggerService;
using System.Text;

namespace CSG.MI.FDW.OpenApi.Infrastructure.Formatter
{
	/// <summary>
	/// Implement generic data list CSV download Service Class
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public class CsvResultService<T> : ICsvResultService<T>
	{
		#region Constructors

		private readonly ILoggerManager _logger;

		public CsvResultService(ILoggerManager logger)
		{
			_logger = logger;
		}

		#endregion

		/// <summary>
		/// Implement generic data list CSV download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		public MemoryStream Download(ICollection<T> data, string fileName)
		{
			_logger.LogInfo("Execute the ICollection<T> list CSV download method");

			try
			{
				var stream = new MemoryStream();

				using (var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true))
				{
					var properties = typeof(T).GetProperties();
					var header = String.Join(",", properties.Select(p => p.Name));
					writer.WriteLine(header);

					var count = 0;

					//_logger.LogInfo("Header 로그: {Header}", header); // header를 로그로 출력

					foreach (var item in data)
					{
						count++;
						var values = properties.Select(p =>
						{
							var value = p.GetValue(item);
							return value != null ? value.ToString() : String.Empty;
						}).ToList();

						/*if (values.Count != properties.Length)
						{
							_logger.LogError("Mismatched item count in row: {ItemCount} (Expected: {ExpectedCount})", values.Count, properties.Length);
						}*/

						var line = String.Join(",", values);

						writer.WriteLine(line);
					}
					writer.Flush();
				}

				stream.Position = 0;
				var result = new MemoryStream(stream.ToArray());
				stream.Dispose();

				_logger.LogInfo("File '{fileName}' downloaded successfully.", fileName);

				return result;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex.Message);
				throw;
			}
		}

		/// <summary>
		/// Implement generic data list CSV download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		public MemoryStream Download(T data, string fileName)
		{
			_logger.LogInfo("Execute the Generic T CSV download method");

			try
			{
				var stream = new MemoryStream();

				using (var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true))
				{
					var header = String.Join(",", typeof(T).GetProperties().Select(p => p.Name));
					writer.WriteLine(header);

					var line = String.Join(",", typeof(T).GetProperties().Select(p =>
					{
						var value = p.GetValue(data);
						return value != null ? value.ToString() : String.Empty;
					}));
					writer.WriteLine(line);

					writer.Flush();
				}

				stream.Position = 0;
				var result = new MemoryStream(stream.ToArray());
				stream.Dispose();

				_logger.LogInfo("File '{fileName}' downloaded successfully.", fileName);

				return result;
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to download file '{fileName}'.", fileName);
				throw;
			}
		}
	}
}
