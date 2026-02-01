//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
// - 2023.04.07 taehyeon.ryu : PostgreSQL Write ToCSV Logic Change 
// - 2023.06.20 taehyeon.ryu : GetData String Type Contains ','(Comma).
// - 2024.01.10 taehyeon.ryu : GetData String Type Contains ','(Coma) -> ' / '
// - 2024.09.23 taehyeon.ryu : GetData String Type Contains '\"' -> ''' and  "`","¶" -> ''
//===============================================================================

using CSG.MI.AutoETL.Models.Extract;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;

namespace CSG.MI.AutoETL.Core.Extract
{
	public abstract class DbServerExtractorBase
	{
		#region Fields

		protected ExtractorConfig _config;

		private string _extractCsvFolder = AppConfig.ExtractCsvFolder;

		#endregion

		#region Constructors

		public DbServerExtractorBase(ExtractorConfig config)
		{
			_config = config;
		}

		#endregion

		#region Pretected Methods

		protected int ToCsv(List<string> lines, Logger logger)
		{
			var total = lines.Count();

			if (lines.Count() > 0)
			{
				var csv = GetCsvFullPath();

				using (var writer = new StreamWriter(csv, false, Encoding.UTF8))
				{
					// Write header
					var header = String.Join(",", _config.QueryCondition.GetColumnNames());
					writer.WriteLine(header);

					// Write extracted data
					foreach (var line in lines)
					{
						writer.WriteLine(line);
					}
				}

				logger.Info(@"[{0}][{1}][{2}]", "DbServerExtractorBase", "Write CSV", $"total {total} lines");
			}
			else
			{
				logger.Warn(@"[{0}][{1}][{2}]", "DbServerExtractorBase", "Write CSV", $"total {total} lines");
			}


			return total;
		}

		protected List<string> GetColumnNames(IDbCommand cmd)
		{
			var columns = new List<string>();

			// Read column names
			using (var reader = cmd.ExecuteReader())
			{
				reader.Read();
				var tableSchema = reader.GetSchemaTable();

				foreach (DataRow row in tableSchema.Rows)
					columns.Add(row["ColumnName"].ToString());
			}

			return columns;
		}

		protected string GetCsvFullPath()
		{
			return Path.Combine(_extractCsvFolder, GetCsvFullFileName(_config.GetFileName()));
		}

		protected string GetCsvFullFileName(string csvFileName)
		{
			return $"{csvFileName}-{_config.Start.ToString(AppConfig.CsvFormat)}-{_config.End.ToString(AppConfig.CsvFormat)}.csv";
		}

		protected dynamic GetData(DbDataReader reader, int index, Type type)
		{
			dynamic t = null;

			if (type == typeof(Boolean))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetBoolean(index);
			else if (type == typeof(DateTime))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetDateTime(index).ToString(AppConfig.PostgreSqlFormat);
			//if (reader.IsDBNull(index)) t = null; else t = $"{reader.GetDateTime(index).ToString(AppConfig.PostgreSqlFormat)}'";
			else if (type == typeof(Decimal))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetDecimal(index);
			else if (type == typeof(Double))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetDouble(index);
			else if (type == typeof(Int16))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetInt16(index);
			else if (type == typeof(Int32))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetInt32(index);
			else if (type == typeof(Int64))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetInt64(index);
			else if (type == typeof(Single))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetFloat(index);
			else if (type == typeof(String))
				//if (reader.IsDBNull(index)) t = null; else t = reader.GetString(index).Trim();
				if (reader.IsDBNull(index)) t = null;
				else
				{
					//If there is ',' inside the existing string, wrap it with ''... -> Error.
					string value = reader.GetString(index).Trim();
                    if (value.Contains(",") || value.Contains("\"") || value.Contains("`") || value.Contains("¶"))
                    {
                        value = value.Replace(",", " ");
                        value = value.Replace("\"", "'");
						value = value.Replace("`", " ");
                        value = value.Replace("¶", " ");
                    }
                    t = value;

                } // String Data Contains ','(Comma) 
			else if (type == typeof(Byte))
				if (reader.IsDBNull(index)) t = null; else t = reader.GetByte(index);
			else if (type == typeof(Byte[]))
			{
				var blob = new Byte[(reader.GetBytes(0, 0, null, 0, int.MaxValue))];
				reader.GetBytes(0, 0, blob, 0, blob.Length);
				if (reader.IsDBNull(index)) t = null; else t = blob;
			}
			else if (type == typeof(SByte))
                if (reader.IsDBNull(index)) t = null; else t = (short)reader.GetByte(index);

            return t;
		}

		#endregion
	}
}
