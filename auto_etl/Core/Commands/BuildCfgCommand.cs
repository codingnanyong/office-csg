//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Configs;
using CSG.MI.AutoETL.Models;
using NLog;
using System;
using System.Diagnostics;
using System.Linq;

namespace CSG.MI.AutoETL.Core.Commands
{
	/// <summary>
	/// ExporterConfigBuilder로 부터 Factory Config File 생성
	/// DbConn, SelectQuery, CSV File Name 정의
	/// </summary>
	public class BuildCfgCommand : ICommand
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		#endregion

		#region Properties

		public CompanyCode Factory { get; private set; }

		#endregion

		#region ICommand Members

		public void Execute()
		{
			_logger.Info(@"[{0}][{1}][{2}]", "BuildCommand", "Begin", Factory.ToString());

			Stopwatch sw = new Stopwatch();
			sw.Start();

			foreach (var key in AppConfig.ExtractSettings.AllKeys)
			{

				var value = AppConfig.ExtractSettings.GetValues(key).FirstOrDefault();
				var array = value.Split(';');
				var companyString = array[0];
				var connId = array[1];

				var dbConnInfo = new DbConnInfo();

				try
				{
					var serverKind = (DbServerKind)Enum.Parse(typeof(DbServerKind), array[2], true);
					var server = array[3];
					var port = Int32.Parse(array[4]);
					var userId = array[5];
					var password = array[6];
					var database = array[7];

					dbConnInfo.Id = connId;
					dbConnInfo.ServerKind = serverKind;
					dbConnInfo.Server = server;
					dbConnInfo.Port = port;
					dbConnInfo.Database = database;
					dbConnInfo.UserId = userId;
					dbConnInfo.Password = password;

					IDbServerConfigBuilder builder = null;
					switch (dbConnInfo.ServerKind)
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
						case DbServerKind.MariaDB:
							builder = new MySQLConfigBuilder();
							break;
						case DbServerKind.MySQL:
							builder = new MySQLConfigBuilder();
							break;
						default:
							break;
					}

					builder.Build(companyString, dbConnInfo);
				}
				catch (Exception ex)
				{
					_logger.Error(@"[{0}][{1}][{2}]", "BuildCommand", "Execute", ex.Message);

					continue;
				}
			}

			sw.Stop();

			_logger.Info(@"[{0}][{1}][{2}]", "BuildCommand", "End", sw.Elapsed);
		}

		#endregion
	}
}
