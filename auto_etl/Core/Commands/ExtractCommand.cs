//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Core.Extract;
using CSG.MI.AutoETL.Models.Extract;
using CSG.MI.AutoETL.Utils;
using NLog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace CSG.MI.AutoETL.Core.Commands
{
	public class ExtractCommand : ICommand
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		#endregion

		#region Properties

		private CompanyCode CompanyCode { get; set; } = CompanyCode.ALL;
		private ProcessMode ProcessMode { get; set; } = ProcessMode.Single;
		public string ConfigDirPath { get; set; }

		#endregion

		#region Constructors

		public ExtractCommand(string configDirPath)
		{
			ConfigDirPath = configDirPath;
		}

		#endregion

		#region ICommand Members

		public void Execute()
		{
			_logger.Info(@"[{0}][{1}][{2}]", "ExtractCommand", "Begin", ConfigDirPath);
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

			// Filter company
			if (CompanyCode != CompanyCode.ALL)
			{
				cfgs = cfgs.Where(x => x.Company == CompanyCode).ToList();
			}

			// Process by Mode
			switch (ProcessMode)
			{
				case ProcessMode.Single:
					ExtractSequentially(cfgs);
					cfgs.ForEach(x =>
					{
						_logger.Info(@"[{0}][{1}][{2}]", "ExtractCommand", (x.IsSuccessfullyCompleted ? "O" : "X"), x.GetFileName());
					});
					break;

				case ProcessMode.Multiple:
					var dic = cfgs.GroupBy(x => x.Category)
						  .OrderBy(x => x.Key)
						  .ToDictionary(x => x.Key, x => x.OrderBy(y => y.GetFileName()).ToList());

					ExtractCategorically(dic);

					foreach (var kvp in dic)
					{
						kvp.Value.ForEach(x =>
						{
							_logger.Info(@"[{0}][{1}][{2}]", "ExtractCommand", (x.IsSuccessfullyCompleted ? "O" : "X"), x.GetFileName());
						});
					}
					break;

				default:
					break;
			}

			sw.Stop();
			_logger.Info(@"[{0}][{1}][{2}]", "ExtractCommand", "End", sw.Elapsed);
		}

		#endregion

		#region Public Methods

		#endregion

		#region Private Methods

		private void ExtractCategorically(Dictionary<string, List<ExtractorConfig>> dic)
		{
			var tasks = new List<Task>();

			foreach (var kvp in dic)
			{
				var task = Task.Run(() =>
				{
					ExtractSequentially(kvp.Value);
				});
				tasks.Add(task);
			}

			Task.WaitAll(tasks.ToArray());
		}

		private void ExtractSequentially(List<ExtractorConfig> cfgs)
		{
			var path = AppConfig.EtlHistoryPath;
			Stopwatch sw = new Stopwatch();
			foreach (var cfg in cfgs.OrderBy(x => x.DbConn.Id))
			{
				sw.Restart();

				// 이력 조회
				var histories = HistoryManager.Instance.GetHistory(path);
				var lastHistory = histories.LastOrDefault(x => x.FileName.Equals(cfg.GetFileName()));

				try
				{
					// 이력이 없다면
					if (lastHistory is null)
					{
						// 추출 기간 설정
						var newHistory = new HistoryRow(cfg);

						// 추출
						Extract(cfg, newHistory);

						// 추출 완료 시간 설정
						newHistory.EllipsedTime = sw.Elapsed;
						newHistory.WriteTime = DateTime.Now;

						// 새로운 이력 추가
						histories.Add(newHistory);
					}
					else
					{
						// 추출 이력이 있고, 새로운 추출 범위라면
						if (lastHistory.UpdateRange(cfg.DeltaValue, cfg.Margin))
						{
							// 추출
							Extract(cfg, lastHistory);

							// 추출 완료 시간 설정
							lastHistory.EllipsedTime = sw.Elapsed;
							lastHistory.WriteTime = DateTime.Now;
						}
						else
						{
							_logger.Warn(@"[{0}][{1}][{2}]", "ExtractCommand", lastHistory.ToString(), "The extraction range is the same");
							continue;
						}
					}

					sw.Stop();
					_logger.Info(@"[{0}][{1}][{2}]", "ExtractCommand", "Extract", sw.Elapsed);
				}
				catch (Exception ex)
				{
					_logger.Error(@"[{0}][{1}][{2}]", "ExtractCommand", "Exception", ex.ToFormattedString());

					continue;
				}

				// 전체 이력 변경
				if (cfg.IsSuccessfullyCompleted)
				{
					HistoryManager.Instance.SetHistory(path, histories);
				}
			}
		}

		private void Extract(ExtractorConfig cfg, HistoryRow newHistory)
		{
			//var start = newHistory.Start;
			//var end = newHistory.End;
			cfg.Start = newHistory.Range.Start;
			cfg.End = newHistory.Range.End;

			try
			{
				IDbServerExtractor extractor = null;

				switch (cfg.DbConn.ServerKind)
				{
					case DbServerKind.SqlServer:
						extractor = new SqlServerExtractor(cfg);
						break;
					case DbServerKind.SqlServerV8:
						extractor = new SqlServerV8Extractor(cfg);
						break;
					case DbServerKind.Oracle:
						extractor = new OracleExtractor(cfg);
						break;
					case DbServerKind.OracleService:
						extractor = new OracleExtractor(cfg);
						break;
					case DbServerKind.PostgreSql:
						extractor = new PostgresExtractor(cfg);
						break;
					case DbServerKind.MariaDB:
						extractor = new MySQLExtractor(cfg);
						break;
					case DbServerKind.MySQL:
						extractor = new MySQLExtractor(cfg);
						break;
					default:
						throw new NotSupportedException("Not Supported ServerKind.");
				}

				// Export Csv
				newHistory.RowCnt = extractor.Extract();

				// Update the final result
				cfg.IsSuccessfullyCompleted = true;
			}
			catch
			{
				// Update the final result
				cfg.IsSuccessfullyCompleted = false;

				throw;
			}
		}

		#endregion
	}
}
