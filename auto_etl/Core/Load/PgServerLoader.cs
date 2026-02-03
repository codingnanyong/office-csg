//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Utils;
using NLog;
using Npgsql;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace CSG.MI.AutoETL.Core.Load
{
	public class PgServerLoader : IDbServerLoader
	{
		#region Fields

		private Logger _logger = LogManager.GetCurrentClassLogger();

		private string _loadCsvFolder = AppConfig.LoadCsvFolder;

		private string _backupFolder = AppConfig.BackupFolder;

		#endregion

		#region Constructors

		public PgServerLoader()
		{
		}

		#endregion

		#region IDbServerImporter Members

		public void Load()
		{
			var csvs = Directory.EnumerateFiles(_loadCsvFolder, $"*.csv", SearchOption.TopDirectoryOnly).OrderBy(x => x);

			foreach (var csv in csvs)
			{
				var ss = Path.GetFileName(csv).Split('-');
				var factory = ss[0]; 
				var connId = ss[1]; 
				var ip = ss[2].Replace('.', '_'); 
				var category = ss[3]; 
				var table = ss[4]; 
				var start = ss[5];
				var end = ss[6];

				// Read CSV Header
				string columns = String.Empty;
				using (var reader = new StreamReader(csv))
				{
					columns = reader.ReadLine();
				}

				//DB Connection

				using (var conn = new NpgsqlConnection(connString))
				{
					//string loadTable = $"{category}{connId}.{table}";
					string loadTable = $"{connId}.{table}";
					string target = _loadCsvFolder;

					try
					{
						conn.Open();

						// Copy 명령에서 컬럼을 지정할 경우, 없는 컬럼은 Default value로 Insert
						// Option (format_name, oids, delimiter_character, null_string, header, quote_character, escape_character, force_quote, force_not_null, encoding_name)
						using (var writer = conn.BeginTextImport($"COPY {loadTable} ({columns}) FROM STDIN WITH (format csv, header true, delimiter ',', null '', quote '`', escape '\\')"))
						{
							writer.Write(File.ReadAllText(csv, Encoding.GetEncoding(932)));
							_logger.Info(@"[{0}][{1}][{2}]", "PgServerLoader", "Copy", $"{csv} --> {loadTable}");
						}

						// Check backup directory
						// var folder = Path.Combine(_backupFolder, DateTime.Today.ToShortDateString());
						var folder = _backupFolder;
						if (Directory.Exists(folder) == false)
						{
							Directory.CreateDirectory(folder);
						}

						// Move the zip file to the backup folder
						target = Path.Combine(folder, Path.GetFileName(csv).Replace(".csv", ".zip"));
						ZipHelper.Zip(csv, target, compressionLevel: 9);
						_logger.Info(@"[{0}][{1}][{2}]", "PgServerLoader", "Backup", $"{csv} --> {target}");

						// Delete the csv file
						FileHelper.DeleteFile(csv);
						_logger.Info(@"[{0}][{1}][{2}]", "PgServerLoader", "Delete", $"{csv}");
					}
					catch
					{
						target = Path.Combine(_loadCsvFolder, AppConfig.FailureFoler, Path.GetFileName(csv));
						FileHelper.Move(csv, target, overwrite: true);

						_logger.Info(@"[{0}][{1}][{2}]", "PgServerLoader", "Move", $"{csv} --> {target}");

						throw;
					}
					finally
					{
						if (conn.State == ConnectionState.Open)
							conn.Close();
					}
				}
			}
		}

		#endregion
	}
}
