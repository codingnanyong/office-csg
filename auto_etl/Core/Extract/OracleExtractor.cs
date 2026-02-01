//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Models.Extract;
using NLog;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace CSG.MI.AutoETL.Core.Extract
{
	public class OracleExtractor : DbServerExtractorBase, IDbServerExtractor
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		#endregion

		#region Constructors

		public OracleExtractor(ExtractorConfig config) : base(config)
		{
		}

		#endregion

		#region IDbServerToCsv Members

		public int Extract()
		{
			string query = GetQuery(_config);
			using (var conn = new OracleConnection(_config.DbConn.ConnectionString))
			{
				try
				{
					conn.Open();

					_logger.Info(@"[{0}][{1}][{2}][{3}]", "Build", "Open DB", conn.DataSource, conn.Database);

					using (var command = new OracleCommand(query, conn))
					{
						command.CommandType = CommandType.Text;
						command.CommandTimeout = AppConfig.CommandTimeout;

						_logger.Info(@"[{0}][{1}][{2}]", "OracleExtractor", _config.GetFileName(), command.CommandText);

						using (var reader = command.ExecuteReader())
						{

							int index = 0; Type t = null;
							var lines = new List<string>();

							while (reader.Read())
							{
								index = 0;
								string line = String.Empty;

								foreach (var dt in _config.QueryCondition.GetDataTypes())
								{
									t = Type.GetType($"System.{dt}");
									line += GetData(reader, index, t) + ",";
									index++;
								}

								line = line.Remove(line.Length - 1); // Remove the last character ','
								lines.Add(line);

								// TODO : 양산 시스템 및 VPN 부하
								//if (totalLines > 10)
								//    break;
							}

							reader.Close();

							var cnt = ToCsv(lines, _logger);

							return cnt;
						}
					}
				}
				catch
				{
					throw;
				}
				finally
				{
					if (conn.State == ConnectionState.Open)
						conn.Close();
				}
			}
		}

		#endregion

		#region Private Methods
		/// <summary>
		/// 히스토리 파일을 기반으로 추출 쿼리 조건절의 Delta값을 채운 쿼리 반환
		/// </summary>
		private string GetQuery(ExtractorConfig config)
		{
			var query = new StringBuilder();
			var condition = config.QueryCondition;
			var start = config.Start;
			var end = config.End;

			query.Append("SELECT").Append(" ");

			foreach (var column in condition.SelectClause)
			{
				if (String.IsNullOrEmpty(column.As))
					query.Append($"{column.Text}, ");
				else
					query.Append($"{column.Text} AS {column.As}, ");
			}

			query.Remove(query.Length - 2, 1); // Remove the last comma(,)

			query.Append($"FROM {condition.FromClause}").Append(" ");

			if (condition.WhereClause != null)
			{
				query.Append($"WHERE {condition.WhereClause}");
				query.Replace("{DeltaField}", _config.DeltaField);
				query.Replace("{StartDate}", start.ToString(AppConfig.OracleQueryFormat));
				query.Replace("{EndDate}", end.ToString(AppConfig.OracleQueryFormat));
			}

			return query.ToString();
		}
		#endregion
	}
}
