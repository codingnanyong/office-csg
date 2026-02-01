//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using FluentFTP;
using NLog;
using System;
using System.Diagnostics;
using System.IO;

namespace CSG.MI.AutoETL.Core.Commands
{
    public class FtpDownloadCommand : ICommand
    {
        #region Fields

        private Logger _logger = NLog.LogManager.GetCurrentClassLogger();

        private string _loadZipFolder = AppConfig.LoadZipFolder;

        #endregion

        #region ICommand Members

        public void Execute()
        {
            _logger.Info(@"[{0}][{1}][{2}]", "FtpDownloadCommand", "Begin", _loadZipFolder);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            FtpClient client = new FtpClient(AppConfig.FtpIp, AppConfig.FtpId, AppConfig.FtpPwd, AppConfig.FtpPort);

            client.Connect();

            DownloadFile(client);

            sw.Stop();

            _logger.Info(@"[{0}][{1}][{2}]", "FtpDownloadCommand", "End", sw.Elapsed);
        }

        #endregion

        #region Private Methods

        private void DownloadFile(FtpClient client)
        {
            foreach (var file in client.GetListing())
            {
                // .zip 파일만 다운로드
                if (Path.GetExtension(file.FullName).ToLowerInvariant() != ".zip")
                {
                    continue;
                }

                string target = _loadZipFolder;

                // Download a file
                var localPath = Path.Combine(_loadZipFolder, Path.GetFileName(file.FullName));
                var remotePath = Path.GetFileName(file.FullName); // 루트 디렉터리 밑에 파일

                FtpStatus status = client.DownloadFile(localPath, remotePath, FtpLocalExists.Overwrite);
                if (status == FtpStatus.Success)
                {
                    client.DeleteFile(remotePath);
                    _logger.Info(@"[{0}][{1}][{2}]", "FtpDownloadCommand", "Download Complete", $"{remotePath}");
                }
                else
                {
                    throw new Exception("FTP Download Error");
                }
            }
        }

        #endregion
    }
}
