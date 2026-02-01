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
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace CSG.MI.AutoETL.Core.Commands
{
    public class UnzipCommand : ICommand
    {
        #region Fields

        private Logger _logger = LogManager.GetCurrentClassLogger();

        private string _loadZipFolder = AppConfig.LoadZipFolder;

        private string _loadCsvFolder = AppConfig.LoadCsvFolder;

        #endregion

        #region Properties

        public string ConfigFile { get; private set; }

        #endregion

        #region ICommand Members

        public void Execute()
        {
            _logger.Info(@"[{0}][{1}][{2}]", "UnzipCommand", "Begin", $"{_loadZipFolder} --> {_loadCsvFolder}");

            Stopwatch sw = new Stopwatch();
            sw.Start();

            // Look for zip files in the zip folder
            var zips = Directory.EnumerateFiles(_loadZipFolder, $"*.zip", SearchOption.TopDirectoryOnly).OrderBy(x => x);

            // Unzip each file to the csv folder
            foreach (var zip in zips)
            {
                string target = _loadCsvFolder;

                try
                {
                    // Decompress the zip file
                    ZipHelper.Unzip(zip, target);
                    _logger.Info(@"[{0}][{1}][{2}]", "UnzipCommand", "Decompress", $"{zip} --> {target}");

                    // Move the zip file to the success folder
                    target = Path.Combine(_loadZipFolder, AppConfig.SuccessFolder, Path.GetFileName(zip));
                    FileHelper.Move(zip, target, overwrite:true);
                    _logger.Info(@"[{0}][{1}][{2}]", "UnzipCommand", "Move", $"{zip} --> {target}");

                    // Delete
                    FileHelper.DeleteFile(target);
                }
                catch
                {
                    // Move the zip file to the failure folder
                    target = Path.Combine(_loadZipFolder, AppConfig.FailureFoler, Path.GetFileName(zip));
                    FileHelper.Move(zip, target, overwrite: true);
                    _logger.Info(@"[{0}][{1}][{2}]", "UnzipCommand", "Move", $"{zip} --> {target}");

                    throw;
                }
            }

            sw.Stop();

            _logger.Info(@"[{0}][{1}][{2}]", "UnzipCommand", "End", sw.Elapsed);
        }

        #endregion
    }
}
