//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: teahyeon.ryu
// 개발일: 2023.04.07
// 수정내역:
// - 2023.04.07 taehyeon.ryu : Initial Version
// - 2024.04.01 taehyeon.ryu : Add DeltaField(Int64 UnixTime)
//===============================================================================

using CSG.MI.AutoETL.Core;
using CSG.MI.AutoETL.Models;
using CSG.MI.AutoETL.Models.Extract;
using CSG.MI.AutoETL.Utils;
using MySql.Data.MySqlClient;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;

namespace CSG.MI.AutoETL.Configs
{
	public class MySQLConfigBuilder : IDbServerConfigBuilder
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		private DateTime _initialStart = new DateTime();

		private DateTime _start = new DateTime();

		private DateTime _end = new DateTime();

		private TimeSpan _delta = new TimeSpan(1, 0, 0, 0);

		private TimeSpan _margin = new TimeSpan(1, 0, 0, 0);

		private List<ExtractorConfig> _configs { get; set; } = new List<ExtractorConfig>();

		#endregion

		#region Constructors

		public MySQLConfigBuilder()
		{
		}

		#endregion

		#region Private Methods

		private List<Column> GetColumns(MySqlConnection conn, out string query, out MySqlCommand cmd, MySQLTableInfo table)
		{
			query = $@"SELECT COLUMN_NAME, DATA_TYPE
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE TABLE_CATALOG = '{table.Db}'
                         AND TABLE_SCHEMA = '{table.Schema}'
                         AND TABLE_NAME = '{table.Name}'
                       ORDER BY ORDINAL_POSITION ASC;";

			cmd = new MySqlCommand
			{
				Connection = conn,
				CommandType = CommandType.Text,
				CommandText = query
			};

			var columns = new List<Column>();
			using (var reader = cmd.ExecuteReader())
			{
				while (reader.Read())
				{
					try
					{
						columns.Add(new Column()
						{
							Text = reader["column_name"].ToString(),
							DataType = DbDataTypeRepository.Instance.GetType(DbServerKind.MariaDB, reader["data_type"].ToString()),
							As = reader["column_name"].ToString()
						});
					}
					catch
					{
						// TODO : DB 타입 변환 예외처리
						throw;
					}

				}

				// Add ETL Column(TRANSFORM_TIME, LOAD_TIME 컬럼은 DDL에서 생성)
				columns.Add(new Column()
				{
					Text = "SYSDATE()",
					DataType = DbType.DateTime,
					As = "EXTRACT_TIME"
				});
			}

			return columns;
		}

		private void SaveXml(CompanyCode factory)
		{
			var list = new List<ExtractorConfig>();
			switch (factory)
			{
				case CompanyCode.VJ:
					list = _configs.Where(x => x.Company == CompanyCode.VJ).ToList<ExtractorConfig>();
					break;
				case CompanyCode.JJ:
					list = _configs.Where(x => x.Company == CompanyCode.JJ).ToList<ExtractorConfig>();
					break;
				case CompanyCode.RJ:
					list = _configs.Where(x => x.Company == CompanyCode.RJ).ToList<ExtractorConfig>();
					break;
				case CompanyCode.QD:
					list = _configs.Where(x => x.Company == CompanyCode.QD).ToList<ExtractorConfig>();
					break;
				case CompanyCode.HQ:
					list = _configs.Where(x => x.Company == CompanyCode.HQ).ToList<ExtractorConfig>();
					break;
				case CompanyCode.ALL:
					list.AddRange(_configs);
					break;
				default:
					break;
			}

			foreach (var cfg in list)
			{
				string path = Path.Combine(AppConfig.ExtractCfgFolder, $"{cfg.GetFileName()}.xml");
				XmlHelper.SaveXml<ExtractorConfig>(path, cfg);
				_logger.Info(@"[{0}][{1}][{2}]", "Build", "Create Config", $"{path}");
			}
		}

		#endregion

		#region Public Methods

		public void Build(string companyString, DbConnInfo dbConnInfo)
		{
			using (var conn = new MySqlConnection(dbConnInfo.ConnectionString))
			{
				try
				{
					conn.Open();

					_logger.Info(@"[{0}][{1}][{2}][{3}]", "Build", "Open DB", conn.DataSource, conn.Database);

					var query = "SELECT table_catalog, table_schema, table_name FROM INFORMATION_SCHEMA.TABLES WHERE engine ='InnoDB' AND TABLE_NAME  = 'at_tag_xy';";
					var cmd = new MySqlCommand
					{
						Connection = conn,
						CommandType = CommandType.Text,
						CommandText = query
					};

					// Get Tables
					var tables = new List<MySQLTableInfo>();
					using (var reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							tables.Add(new MySQLTableInfo()
							{
								Db = reader["table_catalog"].ToString(),
								Schema = reader["table_schema"].ToString(),
								Name = reader["table_name"].ToString()
							});
						}
					}

					foreach (var table in tables)
					{
						var columns = GetColumns(conn, out query, out cmd, table);

						var config = new ExtractorConfig();
						config.Priority = 1;
						config.CompanyString = companyString;
						config.DbConn = dbConnInfo;
						config.QueryCondition = new QueryCondition();
						config.QueryCondition.SelectClause = columns;
						config.QueryCondition.FromClause = $"{table.Schema}.{table.Name}";
						config.QueryCondition.WhereClause = "{DeltaField} >= '{StartDate}' AND {DeltaField} < '{EndDate}'"; // select convert(datetime2,'2008-12-19 17:30:09.1234567', 120);
						config.InitialStart = _initialStart;
						config.Start = _start;
						config.End = _end;
						config.DeltaValue = _delta;
						config.LoadTable = table.Name;
						config.Margin = _margin;
						_configs.Add(config);
					}
				}
				catch (SqlException)
				{
					throw;
				}
				finally
				{
					if (conn.State == ConnectionState.Open)
						conn.Close();
				}
			}

			SaveXml(CompanyCode.ALL);
		}

		public void Update(ExtractorConfig config)
		{
			var dbConnInfo = config.DbConn;
			using (var conn = new MySqlConnection(dbConnInfo.ConnectionString))
			{
				try
				{
					conn.Open();

					_logger.Info(@"[{0}][{1}][{2}][{3}]", "Build", "Update", conn.DataSource, conn.Database);

					//query.Replace("{DeltaField}", _config.DeltaField);

					var query = $"SELECT MIN({config.DeltaField}) AS init_start, COUNT(*) AS cnt FROM {config.QueryCondition.FromClause};";
					var cmd = new MySqlCommand
					{
						Connection = conn,
						CommandTimeout = 300,  // RTLS(MariaDB Read TimeOut : Default 30s)
						CommandType = CommandType.Text,
						CommandText = query
					};

					var path = Path.Combine(AppConfig.ExtractCfgFolder, $"{config.GetFileName()}.xml");

					if (config.DeltaFieldType.Equals(DeltaFieldType.None.ToString()))
					{
						config.InitialStart = new DateTime();
						config.QueryCondition.WhereClause = DbDataTypeRepository.Instance.SqlServerDeltaTypeConverter[DeltaFieldType.None];
					}
					else
					{
						using (var reader = cmd.ExecuteReader())
						{
							_logger.Info(@"Query is Sucess!");
							while (reader.Read())
							{
								var obj = reader["init_start"];
								var cnt = reader["cnt"];
								if (obj is System.DBNull)
								{
									throw new Exception($"Time data does not exist. Row count is {cnt}");
								}

								// Update Query
								if (config.DeltaFieldType.Equals(DeltaFieldType.DateTime.ToString()))
								{
									config.InitialStart = DateTime.Parse(obj.ToString());
								}
								else if (config.DeltaFieldType.Equals(DeltaFieldType.String8.ToString()))
								{
									config.InitialStart = DateTime.ParseExact(obj.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
								}
								else if (config.DeltaFieldType.Equals(DeltaFieldType.String14.ToString()))
								{
									config.InitialStart = DateTime.ParseExact(obj.ToString(), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
								}
								else if (config.DeltaFieldType.Equals(DeltaFieldType.Int64.ToString()))
								{
									config.InitialStart = DateTimeOffset.FromUnixTimeSeconds((long)obj / 1000).DateTime;
                                }
							}
						}
					}

					XmlHelper.SaveXml<ExtractorConfig>(path, config);

					_logger.Info(@"[{0}][{1}][{2}]", "Build", "Update", $"{path}");
				}
				catch (MySqlException e)
				{

					_logger.Error(@"[{0}][{1}]", e.ErrorCode, e.Message);
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
	}
}
