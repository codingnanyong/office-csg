namespace CSG.MI.FDW.LoggerService
{
	public interface ILoggerManager
	{
		void LogInfo(string message);
		void LogInfo(string format, params object[] args);
		void LogInfo(string format, object obj);

		void LogWarn(string message);

		void LogDebug(string message);
		void LogDebug(string message, string option);

		void LogError(string message);
		void LogError(string message, int count, int length);
		void LogError(Exception ex, string message);
		void LogError(Exception ex, string message, string option);
		void LogError(Exception ex, string message, string option1, string option2);
	}
}
