//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: teahyeon.ryu
// 개발일: 2023.04.07
// 수정내역:
// - 2023.04.07 taehyeon.ryu : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Models.Extract;
using NLog;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace CSG.MI.AutoETL.Core.Extract
{
	public class PostgresExtractor : DbServerExtractorBase, IDbServerExtractor
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		#endregion

		#region Constructors

		public PostgresExtractor(ExtractorConfig config) : base(config)
		{
		}

		#endregion

		#region IDbServerToCsv Members

		public int Extract()
		{
			string query = GetQuery(_config);
			using (var conn = new NpgsqlConnection(_config.DbConn.ConnectionString))
			{
				try
				{
					conn.Open();

					_logger.Info(@"[{0}][{1}][{2}][{3}]", "Build", "Open DB", conn.DataSource, conn.Database);

					using (var command = new NpgsqlCommand(query, conn))
					{
						command.CommandType = CommandType.Text;
						command.CommandTimeout = AppConfig.CommandTimeout; //sec

						_logger.Info(@"[{0}][{1}][{2}]", "NpgsqlServerExtractor", _config.GetFileName(), command.CommandText);

                        using (var reader = command.ExecuteReader())
                        {
                            var lines = new List<string>();

                            while (reader.Read())
                            {
                                var lineBuilder = new StringBuilder();

                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    var fieldType = reader.GetFieldType(i);
                                    var data = reader.IsDBNull(i) ? null : reader.GetValue(i);

                                    // Handle binary data
                                    if (fieldType == typeof(byte[]))
                                    {
                                        var binaryData = data as byte[];
                                        if (binaryData != null)
                                        {
                                            data = Convert.ToBase64String(binaryData);
                                        }
                                    }

                                    lineBuilder.Append(data + ",");
                                }

                                var line = lineBuilder.ToString().TrimEnd(',');
                                lines.Add(line);
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
				query.Replace("{StartDate}", start.ToString(AppConfig.PostgreSqlFormat));
				query.Replace("{EndDate}", end.ToString(AppConfig.PostgreSqlFormat));
			}

			return query.ToString();
		}
		#endregion
	}
}
