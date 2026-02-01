//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using System.Collections.Specialized;
using System.Configuration;

namespace CSG.MI.AutoETL.Core
{
	public static class AppConfig
	{
		#region Built-in AppSettings

		public static string Mode => ConfigurationManager.AppSettings.Get("Mode");

		public static string ExtractCfgFolder => ConfigurationManager.AppSettings.Get("ExtractCfgFolder");

		public static string ExtractCsvFolder => ConfigurationManager.AppSettings.Get("ExtractCsvFolder");

		public static string ExtractZipFolder => ConfigurationManager.AppSettings.Get("ExtractZipFolder");

		public static string LoadCfgFolder => ConfigurationManager.AppSettings.Get("LoadCfgFolder");

		public static string LoadCsvFolder => ConfigurationManager.AppSettings.Get("LoadCsvFolder");

		public static string LoadZipFolder => ConfigurationManager.AppSettings.Get("LoadZipFolder");

		public static string BackupFolder => ConfigurationManager.AppSettings.Get("BackupFolder");

		public static string SuccessFolder => ConfigurationManager.AppSettings.Get("SuccessFolder");

		public static string FailureFoler => ConfigurationManager.AppSettings.Get("FailureFolder");

		public static string EtlHistoryPath => ConfigurationManager.AppSettings.Get("EtlHistoryPath");

		public static string FtpIp => ConfigurationManager.AppSettings.Get("FtpIp");

		public static int FtpPort => int.Parse(ConfigurationManager.AppSettings.Get("FtpPort"));

		public static string FtpId => ConfigurationManager.AppSettings.Get("FtpId");

		public static string FtpPwd => ConfigurationManager.AppSettings.Get("FtpPwd");

		public static string FdwServer => ConfigurationManager.AppSettings.Get("FdwServer");

		public static int FdwPort => int.Parse(ConfigurationManager.AppSettings.Get("FdwPort"));

		public static string FdwId => ConfigurationManager.AppSettings.Get("FdwId");

		public static string FdwPwd => ConfigurationManager.AppSettings.Get("FdwPwd");

		public static int CommandTimeout => int.Parse(ConfigurationManager.AppSettings.Get("CommandTimeout"));

		public static string SqlServerQueryFormat => "yyyy-MM-dd HH:mm:ss.fffffff";

		public static string SqlServerV8QueryFormat => "yyyy-MM-dd HH:mm:ss.fff";

		public static string OracleQueryFormat => "yyyy-MM-dd HH:mm:ss.fffffff";

		public static string PostgreSqlFormat => "yyyy-MM-dd HH:mm:ss.fffffff";

		public static string MySQLFormat => "yyyy-MM-dd HH:mm:ss.ffffff";

		public static string HistoryFormat => "yyyy-MM-dd HH:mm:ss.fffffff";

		public static string CsvFormat => "yyyyMMddHHmmssfffffff";

		#endregion

		#region CustomSection Using Built-in Type

		public static NameValueCollection ExtractSettings => ((NameValueCollection)ConfigurationManager.GetSection("extractSettings"));

		#endregion
	}
}
