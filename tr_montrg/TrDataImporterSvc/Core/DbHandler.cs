using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using CSG.MI.TrMontrgSrv.BLL;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.Helpers;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.Json;
using CSG.MI.TrMontrgSrv.SL.RepoServices;
using NLog;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Core
{
    public class DbHandler
    {
        #region Fields

        private static readonly ILoggerManager _logger = new LoggerManager();

        #endregion

        #region Public Methods

        public static DbHandlerResult Process(string zip)
        {
            // 1. Unzip the zip file to a new directory
            string unzippedDir;
            try
            {
                bool result = Unzip(zip, out unzippedDir);
                if (result == false)
                    return DbHandlerResult.UnzipFailure;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return DbHandlerResult.UnzipFailure;
            }

            // 2. Load data from the zip file
            Device device;
            try
            {
                device = Load(unzippedDir);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                Clear(unzippedDir); // Rollback
                RenameToFail(zip);
                return DbHandlerResult.DataLoadFailure;
            }

            // 3. Save temp and medium data to database
            try
            {
                bool result = Save(device);
                if (result == false)
                {
                    Clear(unzippedDir); // Rollback
                    RenameToFail(zip);
                    return DbHandlerResult.DataInsertFailure;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return DbHandlerResult.DataInsertFailure;
            }

            // 4. Delete the newly created directory
            try
            {
                bool result = Clear(unzippedDir);
                if (result == false)
                    return DbHandlerResult.DirDeleteFailure;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return DbHandlerResult.DirDeleteFailure;
            }

            // 6. Move the zip file to the backup directory
            try
            {
                bool result = Move(zip);
                if (result == false)
                    return DbHandlerResult.FileMoveFailure;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return DbHandlerResult.FileMoveFailure;
            }

            return DbHandlerResult.Success;
        }

        #endregion

        #region Private Methods

        private static bool Unzip(string zip, out string dir)
        {
            string currDir = Path.GetDirectoryName(zip);
            string newDir = Path.GetFileNameWithoutExtension(zip);
            string target = Path.Combine(currDir, newDir);
            dir = target;

            // Decompress the zip file
            ZipHelper.Unzip(zip, target, deleteOriginal: false);
            //_logger.Info($"Decompressed: {zip} --> {target}");

            return true;
        }

        private static Device Load(string dir)
        {
            Dictionary<string, string> fileDic = new();

            // Look for zip files in the zip folder
            foreach (string file in Directory.EnumerateFiles(dir, $"*.*", SearchOption.TopDirectoryOnly).OrderBy(x => x))
            {
                string filename = Path.GetFileName(file);
                string kind = filename[filename.IndexOf('-')..];

                switch (kind)
                {
                    case "-cfg.json":
                        fileDic.Add("cfg", file);
                        break;
                    case "-ir.jpg":
                        fileDic.Add("ir", file);
                        break;
                    case "-rgb.jpg":
                        fileDic.Add("rgb", file);
                        break;
                    case "-temp.csv":
                        fileDic.Add("raw", file);
                        break;
                    case "-temp.json":
                        fileDic.Add("temp", file);
                        break;
                }
            }

            // Read Json files only
            using var tempReader = new StreamReader(fileDic["temp"], Encoding.UTF8);
            var tempJsonString = tempReader.ReadToEnd();
            var tempJson = JsonSerializer.Deserialize<TemperatureJson>(tempJsonString);

            using var cfgReader = new StreamReader(fileDic["cfg"], Encoding.UTF8);
            var cfgJsonString = cfgReader.ReadToEnd();
            var cfgJson = JsonSerializer.Deserialize<ConfigJson>(cfgJsonString);
            DateTime timestamp = tempJson.Timestamp.ToDateTime().Value;
            cfgJson.Ymd = timestamp.ToYmdHmsTuple().Item1;
            cfgJson.Hms = timestamp.ToYmdHmsTuple().Item2;

            // Read all files
            Medium tempJsonFile = new()
            {
                Ymd = cfgJson.Ymd,
                Hms = cfgJson.Hms,
                MediumType = "temp",
                DeviceId = cfgJson.DeviceId,
                CaptureDt = Model.Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                FileName = Path.GetFileName(fileDic["temp"]),
                FileType = "json",
                FileSize = new FileInfo(fileDic["temp"]).Length,
                FileContent = ImageHelper.ReadFile(fileDic["temp"])
            };
            Medium cfgJsonFile = new()
            {
                Ymd = cfgJson.Ymd,
                Hms = cfgJson.Hms,
                MediumType = "cfg",
                DeviceId = cfgJson.DeviceId,
                CaptureDt = Model.Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                FileName = Path.GetFileName(fileDic["cfg"]),
                FileType = "json",
                FileSize = new FileInfo(fileDic["cfg"]).Length,
                FileContent = ImageHelper.ReadFile(fileDic["cfg"])
            };
            Medium irImgFile = new()
            {
                Ymd = cfgJson.Ymd,
                Hms = cfgJson.Hms,
                MediumType = "ir",
                DeviceId = cfgJson.DeviceId,
                CaptureDt = Model.Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                FileName = Path.GetFileName(fileDic["ir"]),
                FileType = "jpg",
                FileSize = new FileInfo(fileDic["ir"]).Length,
                FileContent = ImageHelper.ReadFile(fileDic["ir"])
            };
            Medium rgbImgFile = new()
            {
                Ymd = cfgJson.Ymd,
                Hms = cfgJson.Hms,
                MediumType = "rgb",
                DeviceId = cfgJson.DeviceId,
                CaptureDt = Model.Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                FileName = Path.GetFileName(fileDic["rgb"]),
                FileType = "jpg",
                FileSize = new FileInfo(fileDic["rgb"]).Length,
                FileContent = ImageHelper.ReadFile(fileDic["rgb"])
            };
            Medium tempRawFile = new()
            {
                Ymd = cfgJson.Ymd,
                Hms = cfgJson.Hms,
                MediumType = "raw",
                DeviceId = cfgJson.DeviceId,
                CaptureDt = Model.Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                FileName = Path.GetFileName(fileDic["raw"]),
                FileType = "csv",
                FileSize = new FileInfo(fileDic["raw"]).Length,
                FileContent = ImageHelper.ReadFile(fileDic["raw"])
            };

            Device device = tempJson.ToDevice();
            device.Cfgs.Add(cfgJson.ToCfg());
            device.Media.Add(tempJsonFile);
            device.Media.Add(cfgJsonFile);
            device.Media.Add(irImgFile);
            device.Media.Add(rgbImgFile);
            device.Media.Add(tempRawFile);

            return device;
        }

        private static bool Save(Device device)
        {
            using var deviceRepository = RepoFactory.Get<DeviceRepository>().CreateRepo();
            using var frameRepository = RepoFactory.Get<FrameRepository>().CreateRepo();
            using var roiRepository = RepoFactory.Get<RoiRepository>().CreateRepo();
            using var boxRepository = RepoFactory.Get<BoxRepository>().CreateRepo();
            using var mediumRepository = RepoFactory.Get<MediumRepository>().CreateRepo();
            using IDeviceRepo deviceRepo = new DeviceRepo(deviceRepository,
                                                          frameRepository,
                                                          roiRepository,
                                                          boxRepository,
                                                          mediumRepository);
            return deviceRepo.CreateAlways(device);
        }

        private static bool Clear(string dir)
        {
            if (DirectoryHelper.Exists(dir))
                FileHelper.DeleteFiles(dir, "*");

            DirectoryHelper.DeleteIfExist(dir);
            return !DirectoryHelper.Exists(dir);
        }

        private static bool Move(string zip)
        {
            string dest = zip.Replace(AppSettings.WatcherCfg.RootDir, AppSettings.WatcherCfg.BackupDir);
            string filename = Path.GetFileName(dest);
            string year = filename.Substring(0, 4);
            string month = filename.Substring(4, 2);
            string day = filename.Substring(6, 2);
            string deviceRoot = Path.GetDirectoryName(dest);
            dest = Path.Combine(deviceRoot, year, month, day, filename);

            DirectoryHelper.CreateIfNotExist(Path.GetDirectoryName(dest));
            FileHelper.Move(zip, dest, overwrite:true);

            return FileHelper.Exists(dest);
        }

        private static void RenameToFail(string zip)
        {
            // Change file extension to .fail
            string newName = Path.Combine(Path.GetDirectoryName(zip),
                                          $"{Path.GetFileNameWithoutExtension(zip)}.fail");
            FileHelper.Move(zip, newName, overwrite:true);
        }

        #endregion

    }
}
