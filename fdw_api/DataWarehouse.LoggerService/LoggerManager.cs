using NLog;

namespace CSG.MI.FDW.LoggerService
{
	public class LoggerManager : ILoggerManager
	{
		private static readonly ILogger _logger = LogManager.GetCurrentClassLogger();

		private static ILogger logger = LogManager.GetCurrentClassLogger();
		public void LogDebug(string message) => logger.Debug(message);
		public void LogDebug(string message, string fileName) => logger.Debug(message, fileName);

		public void LogError(string message) => logger.Error(message);
		public void LogError(string message, int count, int length) => logger.Error(message, count, length);
		public void LogError(Exception ex, string messge) => logger.Error(ex, messge);
		public void LogError(Exception ex, string message, string option) => logger.Error(ex, message, option);
		public void LogError(Exception ex, string message, string option1, string option2) => logger.Error(ex, message, option1, option2);


		public void LogInfo(string message) => logger.Info(message);
		public void LogInfo(string format, params object[] args) => logger.Info(format, args);
		public void LogInfo(string format, object obj) => logger.Info(format, obj);

		public void LogWarn(string message) => logger.Warn(message);

	}
}
