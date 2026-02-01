//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Utils;
using NLog;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace CSG.MI.AutoETL.Core.Commands
{
    /// <summary>
    /// _csv 폴더의 .csv 파일을 압축 후 _zip 폴더로 이동
    /// .csv 파일은 압축 성공시 _csv\success 폴더로 이동, 실패시 _csv\failure 폴더로 이동
    /// </summary>
    public class ZipCommand : ICommand
    {
        #region Fields

        private Logger _logger = NLog.LogManager.GetCurrentClassLogger();

        private string _extractZipFolder = AppConfig.ExtractZipFolder;

        private string _extractCsvFolder = AppConfig.ExtractCsvFolder;

        #endregion

        #region Properties

        #endregion

        #region ICommand Members

        public void Execute()
        {
            _logger.Info(@"[{0}][{1}][{2}]", "ZipCommand", "Begin", $"{_extractCsvFolder} --> {_extractZipFolder}");

            Stopwatch sw = new Stopwatch();
            sw.Start();

            var dic = new Dictionary<string, string>(); // <csv, zip>

            // Search all csv files and make zip file list
            var csvs = Directory.EnumerateFiles(_extractCsvFolder, "*.csv", SearchOption.TopDirectoryOnly).OrderBy(x => x);
            foreach (var csv in csvs)
            {
                var zipFilename = Path.GetFileName(csv).Replace(".csv", ".zip");
                var zip = Path.Combine(_extractZipFolder, zipFilename);
                dic.Add(csv, zip);
            }

            // Process each csv file
            foreach (var kvp in dic)
            {
                string source = kvp.Key;
                string target = kvp.Value;

                try
                {
                    // Compress the csv file
                    ZipHelper.Zip(source, target, compressionLevel: 9);
                    _logger.Info(@"[{0}][{1}][{2}]", "ZipCommand", "Compress", $"{source} --> {target}");

                    // Move the csv file into the success folder
                    target = Path.Combine(_extractCsvFolder, AppConfig.SuccessFolder, Path.GetFileName(source));
                    FileHelper.Move(source, target, overwrite: true);
                    _logger.Info(@"[{0}][{1}][{2}]", "ZipCommand", "Move", $"{source} --> {target}");

                    // Delete moved file
                    FileHelper.DeleteFile(target);
                    _logger.Info(@"[{0}][{1}][{2}]", "ZipCommand", "Delete", $"{target}");
                }
                catch
                {
                    // Move the csv file into the failure folder
                    target = Path.Combine(_extractCsvFolder, AppConfig.FailureFoler, Path.GetFileName(source));
                    FileHelper.Move(source, target, overwrite: true);
                    _logger.Info(@"[{0}][{1}][{2}]", "ZipCommand", "Move", $"{source} --> {target}");

                    throw;
                }
            }

            sw.Stop();

            _logger.Info(@"[{0}][{1}][{2}]", "ZipCommand", "End", sw.Elapsed);
        }

        #endregion
    }
}
