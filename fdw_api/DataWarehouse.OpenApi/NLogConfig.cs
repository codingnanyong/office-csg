using NLog;
using NLog.Config;
using NLog.Targets;
using LogLevel = NLog.LogLevel;

namespace CSG.MI.FDW.OpenApi
{
	public class NLogConfig
	{
		public static void Setup(string appName, string? path = null)
		{
			// Step 1. Create configuration object
			LoggingConfiguration config = new();

			// Step 2. Create targets and add them to the configuration
			ColoredConsoleTarget consoleTarget = new();
			config.AddTarget("console", consoleTarget);

			FileTarget fileTarget = new();
			config.AddTarget("file", fileTarget);

			FileTarget fileErrorTarget = new();
			config.AddTarget("error", fileErrorTarget);

			// Step 3. Set target properties
			string rootPath = "${basedir}/Log/";

			if (String.IsNullOrEmpty(path) == false)
			{
				rootPath = path.Replace("\\", "/").TrimEnd('/');
			}
			consoleTarget.Layout = "${uppercase:${level}} ${message}";
			fileTarget.FileName = rootPath + "/${shortdate}.log";
			fileTarget.Layout = "${longdate} | ${uppercase:${level}} | ${message}";
			fileErrorTarget.FileName = rootPath + "/${shortdate}.err";
			fileErrorTarget.Layout = "${longdate} | ${uppercase:${level}} | ${message}";

			// Step 4. Define rules
			LoggingRule rule1 = new("*", LogLevel.Trace, consoleTarget);
			config.LoggingRules.Add(rule1);

			LoggingRule rule2 = new("*", LogLevel.Trace, fileTarget);
			config.LoggingRules.Add(rule2);

			LoggingRule rule3 = new("*", LogLevel.Error, fileErrorTarget);
			config.LoggingRules.Add(rule3);

			// Step 5. Activate the configuration
			LogManager.Configuration = config;
		}
	}
}