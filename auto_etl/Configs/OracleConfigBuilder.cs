//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Core;
using CSG.MI.AutoETL.Models;
using CSG.MI.AutoETL.Models.Extract;
using CSG.MI.AutoETL.Utils;
using NLog;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;

namespace CSG.MI.AutoETL.Configs
{
	public class OracleConfigBuilder : IDbServerConfigBuilder
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

		public OracleConfigBuilder() { }

		#endregion

		#region Private Methods

		private List<Column> GetColumns(OracleConnection conn, out string query, out OracleCommand cmd, OracleTableInfo table)
		{
			query = $@"SELECT column_name, data_type FROM all_tab_columns WHERE table_name = '{table.Name}' ORDER BY COLUMN_ID ASC"; // Oracle 문법은 ';' 미포함

			cmd = new OracleCommand
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
					var columnName = reader["column_name"].ToString();
					var columnType = reader["data_type"].ToString();
					_logger.Info(@"[{0}][{1}][{2}][{3}]", "OracleConfigBuilder", "GetColumns", table.Db, table.Name + "." + columnName + "(" + columnType + ")");
					columns.Add(new Column()
					{
						Text = columnName,
						DataType = DbDataTypeRepository.Instance.GetType(DbServerKind.Oracle, columnType),
						As = columnName
					});
				}

				// Add ETL Column(TRANSFORM_TIME, LOAD_TIME 컬럼은 DDL에서 생성)
				columns.Add(new Column()
				{
					Text = "SYSDATE",
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
			using (var conn = new OracleConnection(dbConnInfo.ConnectionString))
			{
				try
				{
					conn.Open();

					_logger.Info(@"[{0}][{1}][{2}][{3}]", "Build", "Open DB", conn.DataSource, conn.Database);

					var query = $"SELECT tablespace_name, table_name FROM all_tables WHERE OWNER = 'PCCLIVE' AND Table_name in('PCC_MST_TOOLING_LOC','PCC_MST_TOOL_NAME','PCC_MST_LOC_TOOL_MAP','PCC_TOOL_PUR','PCC_TOOL_PUR_DEL','PCC_TOOL_SCAN_HIST','PCC_TOOL_UPTL','PCC_TOOL_UPTL_DEL','PCC_MST_PART','PCC_MST_PART_GROUP','PCC_CS_BOM_TAIL')";
					// Default :  WHERE OWNer = '{dbConnInfo.UserId}'
					// Oracle 문법은 ';' 미포함
					// PCC or MES(TableName 지정 해서 할 것) : WHERE owner = 'PCCLIVE' AND TABLE_NAME IN (Table List) - conUser not grant tablespace & tables
					var cmd = new OracleCommand
					{
						Connection = conn,
						CommandType = CommandType.Text,
						CommandText = query
					};

					// Get Tables
					var tables = new List<OracleTableInfo>();
					using (var reader = cmd.ExecuteReader())
					{
						while (reader.Read())
						{
							tables.Add(new OracleTableInfo()
							{
								Db = reader["tablespace_name"].ToString(),
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
						//config.SelectQuery.FromClause = $"{table.Db}.{table.Name}";
						config.QueryCondition.FromClause = $"{table.Name}";
						config.QueryCondition.WhereClause = "{DeltaField} >= to_timestamp('{StartDate}', 'yyyy-mm-dd hh24:mi:ss.ff7') AND {DeltaField} < to_timestamp('{EndDate}', 'yyyy-mm-dd hh24:mi:ss.ff7')";
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
			using (var conn = new OracleConnection(dbConnInfo.ConnectionString))
			{
				try
				{
					conn.Open();

					_logger.Info(@"[{0}][{1}][{2}][{3}]", "Build", "Update", conn.DataSource, conn.Database);

					//query.Replace("{DeltaField}", _config.DeltaField);

					var query = $"SELECT MIN({config.DeltaField}) AS init_start, Count(*) FROM {config.QueryCondition.FromClause}"; // Oracle 문법은 ';' 미포함
					var cmd = new OracleCommand
					{
						Connection = conn,
						CommandType = CommandType.Text,
						CommandText = query
					};

					var path = Path.Combine(AppConfig.ExtractCfgFolder, $"{config.GetFileName()}.xml");

					if (config.DeltaFieldType.Equals(DeltaFieldType.None.ToString()))
					{
						config.InitialStart = new DateTime();
						config.QueryCondition.WhereClause = DbDataTypeRepository.Instance.OracleDeltaTypeConverter[DeltaFieldType.None];
					}
					else
					{

						using (var reader = cmd.ExecuteReader())
						{
							while (reader.Read())
							{
								var obj = reader["init_start"];

								// Update Query
								if (config.DeltaFieldType.Equals(DeltaFieldType.DateTime.ToString()))
								{
									config.InitialStart = DateTime.Parse(obj.ToString());
									config.QueryCondition.WhereClause = DbDataTypeRepository.Instance.OracleDeltaTypeConverter[DeltaFieldType.DateTime];
								}
								else if (config.DeltaFieldType.Equals(DeltaFieldType.String8.ToString()))
								{
									config.InitialStart = DateTime.ParseExact(obj.ToString(), "yyyyMMdd", CultureInfo.InvariantCulture);
									config.QueryCondition.WhereClause = DbDataTypeRepository.Instance.OracleDeltaTypeConverter[DeltaFieldType.String8];
								}
								else if (config.DeltaFieldType.Equals(DeltaFieldType.String14.ToString()))
								{
									config.InitialStart = DateTime.ParseExact(obj.ToString(), "yyyyMMddHHmmss", CultureInfo.InvariantCulture);
									config.QueryCondition.WhereClause = DbDataTypeRepository.Instance.OracleDeltaTypeConverter[DeltaFieldType.String14];
								}
								else if (config.DeltaFieldType.Equals(DeltaFieldType.None.ToString()))
								{
									config.InitialStart = DateTime.Parse(obj.ToString());
									config.QueryCondition.WhereClause = DbDataTypeRepository.Instance.OracleDeltaTypeConverter[DeltaFieldType.None];
								}
							}
						}
					}

					XmlHelper.SaveXml<ExtractorConfig>(path, config);

					_logger.Info(@"[{0}][{1}][{2}]", "Build", "Update", $"{path}");
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
		}

		#endregion
	}
}
