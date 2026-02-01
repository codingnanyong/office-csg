//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
// - 2023.04.07 taehyeon.ryu : PostgreSQL/MySQL(MariaDB) Extractor Add
//===============================================================================

using CommandLine;
using CSG.MI.AutoETL.Core;
using CSG.MI.AutoETL.Core.Commands;
using CSG.MI.AutoETL.Utils;
using NLog;
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;

namespace CSG.MI.AutoETL
{
    /// <summary>
    /// Extractor 설정파일 생성(-r "config" -p ".\extract\_cfg")
    /// Extractor 설정파일 변경(delta, delta type)
    /// Extractor 설정파일 업데이트(-r "update" -p ".\extract\_cfg")
    /// Extract(-r "extract" -p ".\extract\_cfg")
    /// Load(-r "load" -p ".\extract\_cfg")
    /// </summary>
    class Program
	{
		#region Fields

		private static Logger _logger = LogManager.GetCurrentClassLogger();

		#endregion

		#region Entry Point

		public static int Main(string[] args)
		{
			try
			{
				NLogConfig.Setup("AutoETL");

				CultureInfo.CurrentCulture = new CultureInfo("en-US", false);
				_logger.Info("--------------------------------------------------------------------------------------------------");
				_logger.Info(@"[{0}][{1}][{2}]", "AutoETL", "Mode", AppConfig.Mode);
				_logger.Info(@"[{0}][{1}][{2}]", "Settings", "FTP", $"Server:{AppConfig.FtpIp}, Port:{AppConfig.FtpPort} ID:{AppConfig.FtpId}");
				_logger.Info(@"[{0}][{1}][{2}]", "Settings", "FDW", $"Server:{AppConfig.FdwServer}, Port:{AppConfig.FdwPort} ID:{AppConfig.FdwId}");
				_logger.Info(@"[{0}][{1}][{2}]", "AutoETL", "Begin", String.Join(" ", args));

				Stopwatch sw = new Stopwatch();
				sw.Start();

				CreateInitFolders();

				Run(args);

				sw.Stop();
				_logger.Info(@"[{0}][{1}][{2}]", "AutoETL", "End", sw.Elapsed);

				Thread.Sleep(5);

				return (int)ExitCodes.ERROR_SUCCESS;
			}
			catch (Exception ex)
			{
				_logger.Error(@"[{0}][{1}][{2}]", "AutoETL", "Exception", ex.ToFormattedString());
				Thread.Sleep(5);

				return (int)ExitCodes.ERROR_UNKNOWN;
			}
		}

		#endregion

		#region Private Methods

		private static void Run(string[] args)
		{
			Parser.Default.ParseArguments<Options>(args)
				.WithParsed<Options>(o =>
				{
					if (String.Compare(o.Run, RunMode.Config.ToString(), true) == 0)
					{
						RunAsConfigBuilder(o);
					}
					else if (String.Compare(o.Run, RunMode.Update.ToString(), true) == 0)
					{
						RunAsConfigUpdater(o);
					}
					else if (String.Compare(o.Run, RunMode.Load.ToString(), true) == 0)
					{
						RunAsLoader(o);
					}
					else if (String.Compare(o.Run, RunMode.Extract.ToString(), true) == 0)
					{
						RunAsExtractor(o);
					}
				});
		}

		private static void CreateInitFolders()
		{
			var folders = new string[]
			{
				AppConfig.ExtractCfgFolder,
				AppConfig.ExtractCsvFolder,
				AppConfig.ExtractZipFolder,
				AppConfig.LoadCfgFolder,
				AppConfig.LoadCsvFolder,
				AppConfig.LoadZipFolder,
				AppConfig.BackupFolder,
				Path.Combine(AppConfig.ExtractCfgFolder, AppConfig.SuccessFolder),
				Path.Combine(AppConfig.ExtractCfgFolder, AppConfig.FailureFoler),
				Path.Combine(AppConfig.ExtractCsvFolder, AppConfig.SuccessFolder),
				Path.Combine(AppConfig.ExtractCsvFolder, AppConfig.FailureFoler),
				Path.Combine(AppConfig.ExtractZipFolder, AppConfig.SuccessFolder),
				Path.Combine(AppConfig.ExtractZipFolder, AppConfig.FailureFoler),
				Path.Combine(AppConfig.LoadCsvFolder, AppConfig.SuccessFolder),
				Path.Combine(AppConfig.LoadCsvFolder, AppConfig.FailureFoler),
				Path.Combine(AppConfig.LoadZipFolder, AppConfig.SuccessFolder),
				Path.Combine(AppConfig.LoadZipFolder, AppConfig.FailureFoler)
			};

			foreach (var folder in folders)
			{
				var directoryInfo = DirectoryHelper.CreateIfNotExist(folder);
			}
		}

		private static void RunAsExtractor(Options options)
		{
			//Export CSV, Update EtlHistory
			ICommand command = new ExtractCommand(options.ConfigDirPath);
			command.Execute();

			//Csv to Zip
			command = new ZipCommand();
			command.Execute();

			//Upload zip
			command = new FtpUploadCommand();
			command.Execute();
		}

		private static void RunAsLoader(Options options)
		{
			ICommand command = new FtpDownloadCommand();
			command.Execute();

			command = new UnzipCommand();
			command.Execute();

			command = new LoadCommand();
			command.Execute();
		}

		private static void RunAsConfigBuilder(Options options)
		{
			ICommand command = new BuildCfgCommand();
			command.Execute();
		}

		private static void RunAsConfigUpdater(Options options)
		{
			ICommand command = new UpdateCfgCommand(options.ConfigDirPath);
			command.Execute();
		}

		#endregion
	}
}
