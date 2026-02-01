//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Core;
using System;
using System.Xml.Serialization;

namespace CSG.MI.AutoETL.Models
{
	[XmlRoot]
	public class DbConnInfo
	{
		#region Properties

		public string Id { get; set; }

		public DbServerKind ServerKind { get; set; }

		public string Server { get; set; }

		public int Port { get; set; }

		public string Database { get; set; }

		public string UserId { get; set; }

		public string Password { get; set; }

		#endregion

		#region Public Methods

		[XmlIgnore]
		public string ConnectionString
		{
			get
			{
				string connString = String.Empty;

				if (ServerKind == DbServerKind.SqlServer)
				{
					connString = $"Server={Server}{(Port == 1433 ? String.Empty : "," + Port)};Database={Database};User Id={UserId};Password={Password};";
				}
				else if (ServerKind == DbServerKind.SqlServerV8)
				{
					connString = $"Server={Server}{(Port == 1433 ? String.Empty : "," + Port)};Database={Database};User Id={UserId};Password={Password};";
				}
				else if (ServerKind == DbServerKind.Oracle)
				{
					connString = $"Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={Server})(PORT={Port})))(CONNECT_DATA=(SID={Database})));User Id={UserId};Password={Password};";
				}
				else if (ServerKind == DbServerKind.OracleService)
				{
					connString = $"Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={Server})(PORT={Port}))(CONNECT_DATA=(SERVICE_NAME={Database})));User Id={UserId};Password={Password};";
				}
				else if (ServerKind == DbServerKind.PostgreSql)
				{
					connString = $"Server={Server};Port={Port};Database={Database};User Id={UserId};Password={Password};";
				}
				else if (ServerKind == DbServerKind.MariaDB || ServerKind == DbServerKind.MySQL)
				{
					connString = $"server={Server};port={Port};database={Database};user ={UserId};password={Password}";
				}

				return connString;
			}
		}

		#endregion
	}
}
