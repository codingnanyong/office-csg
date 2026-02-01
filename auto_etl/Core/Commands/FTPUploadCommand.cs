//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: daekwang.kim
// 개발일: 2021.04.22
// 수정내역:
// - 2021.04.22 daekwang.kim : Initial Version
//===============================================================================

using CSG.MI.AutoETL.Utils;
using FluentFTP;
using NLog;
using System;
using System.Diagnostics;
using System.IO;

namespace CSG.MI.AutoETL.Core.Commands
{
    public class FtpUploadCommand : ICommand
    {
        #region Fields

        private Logger _logger = NLog.LogManager.GetCurrentClassLogger();

        private string _extractZipFolder = AppConfig.ExtractZipFolder;

        #endregion

        #region ICommand Members

        public void Execute()
        {
            _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Begin", _extractZipFolder);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            FtpClient client = new FtpClient(AppConfig.FtpIp, AppConfig.FtpId, AppConfig.FtpPwd, AppConfig.FtpPort);
            client.Connect();

            var zips = Directory.EnumerateFiles(_extractZipFolder, "*.zip", SearchOption.TopDirectoryOnly);
            foreach (var zip in zips)
            {
                UploadFile(client, Path.GetFullPath(zip)); // Example: .\extract\_zip\factory-data-20210602-20210603.zip
            }

            sw.Stop();

            _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "End", sw.Elapsed);
        }

        #endregion

        #region Private Methods

        private void UploadFile(FtpClient client, string localPath)
        {
            string target = _extractZipFolder;
            string zipFile = Path.GetFileName(localPath); // Example: factory-data-20210603-20210604.zip

            try
            {
                // 파일 업로드
                FtpStatus status = client.UploadFile(localPath, Path.GetFileName(localPath), FtpRemoteExists.Overwrite, true);
                if (status == FtpStatus.Success)
                {
                    _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Upload", $"{localPath} --> {Path.GetFileName(localPath)}");

                    // Move the zip file to the success folder
                    target = Path.Combine(_extractZipFolder, AppConfig.SuccessFolder, Path.GetFileName(localPath));
                    FileHelper.Move(localPath, target, overwrite: true);
                    _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Move", $"{localPath} --> {target}");

                    // 클라이언트 저장 용량 제한으로 삭제
                    FileHelper.DeleteFile(target);
                    _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Delete", $"{target}");
                }
                else
                {
                    _logger.Error(@"[{0}][{1}][{2}]", "FtpUploadCommand", "File upload failed", $"{localPath}");

                    throw new Exception($"File upload failed({localPath}");
                }
            }
            catch (Exception e)
            {
                // Move the zip file to the failure folder
                target = Path.Combine(_extractZipFolder, AppConfig.FailureFoler, Path.GetFileName(localPath));
                FileHelper.Move(localPath, target, overwrite: true);

                _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Exception", $"{e.ToFormattedString()}");
            }
        }

        //private void UploadFile(FtpClient client, string localPath)
        //{
        //    string target = _extractZipFolder;

        //    // 로컬 파일 확장자 변경(.zip -> .tmp)
        //    string tmpLocalPath = FileHelper.ChangeExtension(localPath, ".tmp"); // Example: C:\path\to\data.tmp
        //    string zipFile = Path.GetFileName(localPath); // Example: factory-data.zip
        //    string tmpFile = Path.GetFileName(tmpLocalPath); // Example: factory-data.tmp

        //    try
        //    {
        //        // 파일 업로드(.tmp)
        //        FtpStatus status = client.UploadFile(tmpLocalPath, Path.GetFileName(tmpLocalPath), FtpRemoteExists.Overwrite, true);
        //        if (status == FtpStatus.Success)
        //        {
        //            _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Upload", $"{tmpLocalPath} --> {Path.GetFileName(tmpLocalPath)}");

        //            // 원격 파일 확장자 변경(.tmp -> .zip) : 서버 로직은 .zip인 파일만 다운로드
        //            client.Rename(tmpFile, zipFile);
        //            _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Remote Rename", $"{tmpFile} --> {zipFile}");

        //            // 로컬 파일 확장자 변경(.tmp -> .zip)
        //            localPath = FileHelper.ChangeExtension(tmpLocalPath, ".zip");
        //            _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Local Rename", $"{tmpLocalPath} --> {localPath}");

        //            // Move the zip file to the success folder
        //            target = Path.Combine(_extractZipFolder, AppConfig.SuccessFolder, Path.GetFileName(localPath));
        //            FileHelper.Move(localPath, target, overwrite: true);
        //            _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Move", $"{localPath} --> {target}");

        //            // 클라이언트 저장 용량 제한으로 삭제
        //            FileHelper.DeleteFile(target);
        //            _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Delete", $"{target}");
        //        }
        //        else
        //        {
        //            _logger.Error(@"[{0}][{1}][{2}]", "FtpUploadCommand", "File upload failed", $"{localPath}");

        //            throw new Exception($"File upload failed({localPath}");
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        // Move the zip file to the failure folder
        //        target = Path.Combine(_extractZipFolder, AppConfig.FailureFoler, Path.GetFileName(tmpLocalPath));
        //        FileHelper.Move(localPath, target, overwrite: true);

        //        _logger.Info(@"[{0}][{1}][{2}]", "FtpUploadCommand", "Exception", $"{e.ToFormattedString()}");
        //    }
        //}
        #endregion
    }
}
