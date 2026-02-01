//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Configs;
using CSG.MI.AutoETL.Models.Extract;
using CSG.MI.AutoETL.Utils;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace CSG.MI.AutoETL.Core.Commands
{
	public class UpdateCfgCommand : ICommand
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		#endregion

		#region Properties

		public CompanyCode Factory { get; private set; }
		public string ConfigDirPath { get; set; }

		#endregion

		#region Constructors

		public UpdateCfgCommand(string configDirPath)
		{
			ConfigDirPath = configDirPath;
		}

		#endregion

		#region ICommand Members

		public void Execute()
		{
			_logger.Info(@"[{0}][{1}][{2}]", "UpdateCfgCommand", "Begin", Factory.ToString());

			Stopwatch sw = new Stopwatch();
			sw.Start();

			var files = Directory.GetFiles(ConfigDirPath, "*.xml").OrderBy(x => x);
			var cfgs = new List<ExtractorConfig>();

			// Load a configuration files
			foreach (var file in files)
			{
				cfgs.Add(XmlHelper.LoadXML<ExtractorConfig>(file));
				_logger.Info(@"[{0}][{1}][{2}]", "ExtractCommand", "Load Config", file);
			}

			if (cfgs.Count == 0)
			{
				throw new Exception("A default config file is required");
			}
			else
			{
				var filteredResults = FilterConfig(cfgs);

				foreach (var cfg in filteredResults)
				{
					try
					{
						// [DeltaField]가 디폴트 값인 delta 설정파일은 건너띔
						IDbServerConfigBuilder builder = null;
						switch (cfg.DbConn.ServerKind)
						{
							case DbServerKind.None:
								break;
							case DbServerKind.SqlServer:
								builder = new SqlServerConfigBuilder();
								break;
							case DbServerKind.SqlServerV8:
								builder = new SqlServerV8ConfigBuilder();
								break;
							case DbServerKind.Oracle:
								builder = new OracleConfigBuilder();
								break;
							case DbServerKind.OracleService:
								builder = new OracleConfigBuilder();
								break;
							case DbServerKind.PostgreSql:
								builder = new PostgresConfigBuilder();
								break;
							case DbServerKind.MySQL:
								builder = new MySQLConfigBuilder();
								break;
							case DbServerKind.MariaDB:
								builder = new MySQLConfigBuilder();
								break;
							default:
								break;
						}

						builder.Update(cfg);

						MoveSuccessFolder(cfg);

					}
					catch (Exception ex)
					{
						_logger.Error(@"[{0}][{1}][{2}]", "UpdateCfgCommand", "Execute", ex.Message);

						MoveFailureFolder(cfg);

						continue;
					}
				}
			}

			sw.Stop();

			_logger.Info(@"[{0}][{1}][{2}]", "UpdateCfgCommand", "End", sw.Elapsed);
		}

		#endregion

		#region Private Methods

		private List<ExtractorConfig> FilterConfig(List<ExtractorConfig> configs)
		{
			var filteredResults = new List<ExtractorConfig>();
			foreach (var cfg in configs)
			{
				filteredResults.Add(cfg);
			}

			return filteredResults;
		}

		private static void MoveFailureFolder(ExtractorConfig cfg)
		{
			var src = Path.Combine(AppConfig.ExtractCfgFolder, $"{cfg.GetFileName()}.xml");
			var dest = Path.Combine(AppConfig.ExtractCfgFolder, AppConfig.FailureFoler, $"{cfg.GetFileName()}.xml");
			FileHelper.Move(src, dest, overwrite: true);
		}

		private static void MoveSuccessFolder(ExtractorConfig cfg)
		{
			var src = Path.Combine(AppConfig.ExtractCfgFolder, $"{cfg.GetFileName()}.xml");
			var dest = Path.Combine(AppConfig.ExtractCfgFolder, AppConfig.SuccessFolder, $"{cfg.GetFileName()}.xml");
			FileHelper.Move(src, dest, overwrite: true);
		}

		#endregion
	}
}
