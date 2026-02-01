//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : 초기버전
//===============================================================================

using NLog;
using NLog.Config;
using NLog.Targets;

namespace CSG.MI.AutoETL
{
	/// <summary>
	/// Configure NLog in code
	/// </summary>
	public static class NLogConfig
	{
		public static void Setup(string appName)
		{
			// Step 1. Create configuration object
			LoggingConfiguration config = new LoggingConfiguration();

			// Step 2. Create targets and add them to the configuration
			ColoredConsoleTarget consoleTarget = new ColoredConsoleTarget();
			config.AddTarget("console", consoleTarget);

			FileTarget fileTarget = new FileTarget();
			config.AddTarget("file", fileTarget);

			FileTarget fileErrorTarget = new FileTarget();
			config.AddTarget("error", fileErrorTarget);

			// Step 3. Set target properties
			consoleTarget.Layout = "${uppercase:${level}} ${message}";
			fileTarget.FileName = "${basedir}/Log/" + appName + "/${shortdate}.log";
			fileTarget.Layout = "${longdate} | ${uppercase:${level}} | ${message}";
			fileErrorTarget.FileName = "${basedir}/Log/" + appName + "/${shortdate}.err";
			fileErrorTarget.Layout = "${longdate} | ${uppercase:${level}} | ${message}";

			// Step 4. Define rules
			LoggingRule rule1 = new LoggingRule("*", LogLevel.Trace, consoleTarget);
			config.LoggingRules.Add(rule1);

			LoggingRule rule2 = new LoggingRule("*", LogLevel.Trace, fileTarget);
			config.LoggingRules.Add(rule2);

			LoggingRule rule3 = new LoggingRule("*", LogLevel.Error, fileErrorTarget);
			config.LoggingRules.Add(rule3);

			// Step 5. Activate the configuration
			LogManager.Configuration = config;
		}
	}
}
