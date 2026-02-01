//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Core;
using NLog;
using System;
using System.Collections.Generic;
using System.Data;

namespace CSG.MI.AutoETL.Configs
{
	/// <summary>
	/// Convert from DBMS type to .NET Framework type
	/// </summary>
	public class DbDataTypeRepository
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		#endregion

		#region Properites

		public Dictionary<string, DbType> SqlServerTypes { get; set; }
		public Dictionary<string, DbType> SqlServerV8Types { get; set; }
		public Dictionary<string, DbType> OracleTypes { get; set; }
		public Dictionary<string, DbType> PostgresTypes { get; set; }
		public Dictionary<string, DbType> MySQLTypes { get; set; }

		public Dictionary<DeltaFieldType, string> SqlServerDeltaTypeConverter { get; set; }
		public Dictionary<DeltaFieldType, string> SqlServerV8DeltaTypeConverter { get; set; }
		public Dictionary<DeltaFieldType, string> OracleDeltaTypeConverter { get; set; }

		#endregion

		#region Singleton

		private DbDataTypeRepository()
		{
			// <DBMS Type : .Net Type> Lookup Table
			// SQLServer
			SqlServerTypes = new Dictionary<string, DbType>();
			SqlServerTypes.Add("decimal", DbType.Decimal);
			SqlServerTypes.Add("numeric", DbType.Decimal);
			SqlServerTypes.Add("bigint", DbType.Int64);
			SqlServerTypes.Add("smallint", DbType.Int16);
			SqlServerTypes.Add("int", DbType.Int32);
			SqlServerTypes.Add("tinyint", DbType.Byte);
			SqlServerTypes.Add("float", DbType.Double);
			SqlServerTypes.Add("varchar", DbType.String);
			SqlServerTypes.Add("nvarchar", DbType.String);
			SqlServerTypes.Add("nchar", DbType.String);
			SqlServerTypes.Add("char", DbType.String);
			SqlServerTypes.Add("varbinary", DbType.String);
			SqlServerTypes.Add("datetime", DbType.DateTime);
			SqlServerTypes.Add("date", DbType.DateTime);
			SqlServerTypes.Add("image", DbType.Binary);


			// SQLSeverV8
			SqlServerV8Types = new Dictionary<string, DbType>();
			SqlServerV8Types.Add("decimal", DbType.Decimal);
			SqlServerV8Types.Add("numeric", DbType.Decimal);
			SqlServerV8Types.Add("bigint", DbType.Int64);
			SqlServerV8Types.Add("smallint", DbType.Int16);
			SqlServerV8Types.Add("int", DbType.Int32);
			SqlServerV8Types.Add("tinyint", DbType.Byte);
			SqlServerV8Types.Add("float", DbType.Double);
			SqlServerV8Types.Add("varchar", DbType.String);
			SqlServerV8Types.Add("nvarchar", DbType.String);
			SqlServerV8Types.Add("nchar", DbType.String);
			SqlServerV8Types.Add("char", DbType.String);
			SqlServerV8Types.Add("varbinary", DbType.String);
			SqlServerV8Types.Add("datetime", DbType.DateTime);
			SqlServerV8Types.Add("date", DbType.DateTime);
			SqlServerV8Types.Add("image", DbType.Binary);

			// Oracle
			OracleTypes = new Dictionary<string, DbType>();
			OracleTypes.Add("blob", DbType.Object);
			OracleTypes.Add("char", DbType.String);
			OracleTypes.Add("nchar", DbType.String);
			OracleTypes.Add("varchar2", DbType.String);
			OracleTypes.Add("nvarchar2", DbType.String);
			OracleTypes.Add("long", DbType.String);
			OracleTypes.Add("rowid", DbType.String);
			OracleTypes.Add("clob", DbType.String);
			OracleTypes.Add("nclob", DbType.String);
			OracleTypes.Add("float", DbType.Decimal);
			OracleTypes.Add("number", DbType.Decimal);
			OracleTypes.Add("integer", DbType.Decimal);
			OracleTypes.Add("date", DbType.DateTime);
			OracleTypes.Add("timestamp(0)", DbType.DateTime);
			OracleTypes.Add("timestamp(2)", DbType.DateTime);
			OracleTypes.Add("timestamp(6)", DbType.DateTime);
			OracleTypes.Add("timestamp(7)", DbType.DateTime);
			OracleTypes.Add("timestamp(9)", DbType.DateTime);
			OracleTypes.Add("raw", DbType.Binary);

			//PostgreSQL
			PostgresTypes = new Dictionary<string, DbType>();
			PostgresTypes.Add("numeric", DbType.Decimal);
			PostgresTypes.Add("bigint", DbType.Int64); ;
			PostgresTypes.Add("varchar", DbType.String);
			PostgresTypes.Add("character varying", DbType.String);
			PostgresTypes.Add("timestamp without time zone", DbType.DateTime);
			PostgresTypes.Add("datetime", DbType.DateTime);
			PostgresTypes.Add("date", DbType.DateTime);
			PostgresTypes.Add("bigserial", DbType.Int64);
			PostgresTypes.Add("character", DbType.String);
            PostgresTypes.Add("real", DbType.Single);
			PostgresTypes.Add("bytea",DbType.Binary);
            PostgresTypes.Add("integer", DbType.Int32);
            PostgresTypes.Add("boolean", DbType.Boolean);

            //MariaDB or MySQL
            MySQLTypes = new Dictionary<string, DbType>();
			MySQLTypes.Add("varchar", DbType.String);
			MySQLTypes.Add("datetime", DbType.DateTime);
			MySQLTypes.Add("bigint", DbType.Int64);
			MySQLTypes.Add("tinyint", DbType.SByte);
			MySQLTypes.Add("int", DbType.Int32);
			MySQLTypes.Add("double", DbType.Double);
			MySQLTypes.Add("float", DbType.Single);
			MySQLTypes.Add("blob", DbType.Byte);
			MySQLTypes.Add("char", DbType.String);


			// DeltaField Converters
			SqlServerDeltaTypeConverter = new Dictionary<DeltaFieldType, string>();
			SqlServerDeltaTypeConverter.Add(DeltaFieldType.DateTime, "{DeltaField} >= convert(datetime2,'{StartDate}', 120) AND {DeltaField} < convert(datetime2,'{EndDate}', 120)");
			SqlServerDeltaTypeConverter.Add(DeltaFieldType.String8, "convert(datetime2, {DeltaField}, 120) >= convert(datetime2,'{StartDate}', 120) AND convert(datetime2, {DeltaField}, 120) < convert(datetime2,'{EndDate}', 120)");
			SqlServerDeltaTypeConverter.Add(DeltaFieldType.String14, "convert(datetime2, stuff(stuff(stuff({DeltaField}, 9, 0, ''), 12,0, ':'), 15, 0, ':')) >= convert(datetime2,'{StartDate}', 120) AND convert(datetime2, stuff(stuff(stuff({DeltaField}, 9, 0, ''), 12,0, ':'), 15, 0, ':')) < convert(datetime2,'{EndDate}', 120)");
			SqlServerDeltaTypeConverter.Add(DeltaFieldType.None, "1=1");

			SqlServerV8DeltaTypeConverter = new Dictionary<DeltaFieldType, string>();
			SqlServerV8DeltaTypeConverter.Add(DeltaFieldType.DateTime, "{DeltaField} >= convert(datetime,'{StartDate}', 120) AND {DeltaField} < convert(datetime,'{EndDate}', 120)");
			SqlServerV8DeltaTypeConverter.Add(DeltaFieldType.String8, "convert(datetime2, {DeltaField}, 120) >= convert(datetime2,'{StartDate}', 120) AND convert(datetime2, {DeltaField}, 120) < convert(datetime2,'{EndDate}', 120)");
			SqlServerV8DeltaTypeConverter.Add(DeltaFieldType.String14, "convert(datetime2, stuff(stuff(stuff({DeltaField}, 9, 0, ''), 12,0, ':'), 15, 0, ':')) >= convert(datetime2,'{StartDate}', 120) AND convert(datetime2, stuff(stuff(stuff({DeltaField}, 9, 0, ''), 12,0, ':'), 15, 0, ':')) < convert(datetime2,'{EndDate}', 120)");
			SqlServerV8DeltaTypeConverter.Add(DeltaFieldType.None, "1=1");

			OracleDeltaTypeConverter = new Dictionary<DeltaFieldType, string>();
			OracleDeltaTypeConverter.Add(DeltaFieldType.DateTime, "{DeltaField} >= to_timestamp('{StartDate}', 'yyyy-mm-dd hh24:mi:ss.ff7') AND {DeltaField} < to_timestamp('{EndDate}', 'yyyy-mm-dd hh24:mi:ss.ff7')");
			OracleDeltaTypeConverter.Add(DeltaFieldType.String8, "to_timestamp({DeltaField}, 'yyyymmdd') >= to_timestamp('{StartDate}', 'yyyy-mm-dd hh24:mi:ss.ff7') AND to_timestamp({DeltaField}, 'yyyymmdd') < to_timestamp('{EndDate}', 'yyyy-mm-dd hh24:mi:ss.ff7')");
			OracleDeltaTypeConverter.Add(DeltaFieldType.String14, "to_timestamp({DeltaField}, 'yyyymmddhh24miss') >= to_timestamp('{StartDate}', 'yyyy-mm-dd hh24:mi:ss.ff7') AND to_timestamp({DeltaField}, 'yyyymmddhh24miss') < to_timestamp('{EndDate}', 'yyyy-mm-dd hh24:mi:ss.ff7')");
			OracleDeltaTypeConverter.Add(DeltaFieldType.None, "1=1");
		}

		private static readonly Lazy<DbDataTypeRepository> _instance = new Lazy<DbDataTypeRepository>(() => new DbDataTypeRepository());

		public static DbDataTypeRepository Instance { get { return _instance.Value; } }

		#endregion

		#region Public Methods

		public DbType GetType(DbServerKind kind, string dataType)
		{
			DbType ret = DbType.Object;

			try
			{
				switch (kind)
				{
					case DbServerKind.None:
						break;
					case DbServerKind.SqlServer:
						ret = SqlServerTypes[dataType.ToLower()];
						break;
					case DbServerKind.SqlServerV8:
						ret = SqlServerV8Types[dataType.ToLower()];
						break;
					case DbServerKind.Oracle:
						ret = OracleTypes[dataType.ToLower()];
						break;
					case DbServerKind.OracleService:
						ret = OracleTypes[dataType.ToLower()];
						break;
					case DbServerKind.PostgreSql:
						ret = PostgresTypes[dataType.ToLower()];
						break;
					case DbServerKind.MariaDB:
						ret = MySQLTypes[dataType.ToLower()];
						break;
					case DbServerKind.MySQL:
						ret = MySQLTypes[dataType.ToLower()];
						break;
					default:
						break;
				}
			}
			catch (Exception ex)
			{
				// TODO : DB 타입 변환 예외처리
				_logger.Info(@"[{0}][{1}][{2}][{3}]", "DbDataTypeConverter", kind, dataType, ex.Message);
				throw;
			}

			return ret;

		}

		#endregion
	}
}
