using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.Helpers;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.Json;
using CSG.MI.TrMontrgSrv.SL.RepoServices;

namespace CSG.MI.TrMontrgSrv.EF.Test.Fixtures
{
    /// <summary>
    /// Database Fixture
    /// </summary>
    public class DatabaseFixture : IDisposable
    {
        #region Fields

        private bool _disposed;
        private readonly List<Tuple<TemperatureJson, ConfigJson, Medium, Medium, Medium, Medium, Medium>> _data;

        #endregion

        #region Constructors

        public DatabaseFixture()
        {
            _data = ReadData();
            DeleteData();
            CreateData();
        }

        #endregion

        #region Properties

        public Repo<DeviceRepository> DeviceFixture { get; private set; } = RepoFactory.Get<DeviceRepository>();

        public Repo<FrameRepository> FrameFixture { get; private set; } = RepoFactory.Get<FrameRepository>();

        public Repo<RoiRepository> RoiFixture { get; private set; } = RepoFactory.Get<RoiRepository>();

        public Repo<BoxRepository> BoxFixture { get; private set; } = RepoFactory.Get<BoxRepository>();

        public Repo<CfgRepository> CfgFixture { get; private set; } = RepoFactory.Get<CfgRepository>();

        public Repo<MediumRepository> MediumFixture { get; private set; } = RepoFactory.Get<MediumRepository>();

        public List<Device> Devices { get; private set; } = new List<Device>();

        public List<Frame> Frames { get; private set; } = new List<Frame>();

        public List<Roi> Rois { get; private set; } = new List<Roi>();

        public List<Box> Boxes { get; private set; } = new List<Box>();

        public List<Cfg> Cfgs { get; private set; } = new List<Cfg>();

        public List<Medium> Media { get; private set; } = new List<Medium>();

        #endregion

        #region Private Methods

        private static List<Tuple<TemperatureJson, ConfigJson, Medium, Medium, Medium, Medium, Medium>> ReadData()
        {
            var baseDir = AppDomain.CurrentDomain.BaseDirectory;
            Dictionary<string, List<string>> pathDic = new()
            {
                {
                    "20210807_110748",
                    new List<string>()
                    {
                        // TRIOT-A001, TR03, JJ
                        Path.Combine(baseDir, "DataSet", "20210807_110748-temp.json"),
                        Path.Combine(baseDir, "DataSet", "20210807_110748-cfg.json"),
                        Path.Combine(baseDir, "DataSet", "20210807_110748-ir.jpg"),
                        Path.Combine(baseDir, "DataSet", "20210807_110748-rgb.jpg"),
                        Path.Combine(baseDir, "DataSet", "20210807_110748-temp.csv")
                    }
                },
                {
                    "20210807_111759",
                    new List<string>()
                    {
                        // TRIOT-A002, TR01, JJ
                        Path.Combine(baseDir, "DataSet", "20210807_111759-temp.json"),
                        Path.Combine(baseDir, "DataSet", "20210807_111759-cfg.json"),
                        Path.Combine(baseDir, "DataSet", "20210807_111759-ir.jpg"),
                        Path.Combine(baseDir, "DataSet", "20210807_111759-rgb.jpg"),
                        Path.Combine(baseDir, "DataSet", "20210807_111759-temp.csv")
                    }
                }
            };

            // Tuple<temp_json_object, cfg_json_object, temp_json, cfg_json, ir_image, rgb_imge, temp_csv>
            var data = new List<Tuple<TemperatureJson, ConfigJson, Medium, Medium, Medium, Medium, Medium>>();

            foreach (var kvp in pathDic)
            {
                // Read Json files
                using var tempReader = new StreamReader(kvp.Value[0], Encoding.UTF8);
                var tempJsonString = tempReader.ReadToEnd();
                var tempJson = JsonSerializer.Deserialize<TemperatureJson>(tempJsonString);

                using var cfgReader = new StreamReader(kvp.Value[1], Encoding.UTF8);
                var cfgJsonString = cfgReader.ReadToEnd();
                var cfgJson = JsonSerializer.Deserialize<ConfigJson>(cfgJsonString);
                cfgJson.Ymd = kvp.Key.Substring(0, 8);
                cfgJson.Hms = kvp.Key.Substring(9, 6);

                // Read all files
                Medium tempJsonFile = new()
                {
                    Ymd = cfgJson.Ymd,
                    Hms = cfgJson.Hms,
                    MediumType = "temp",
                    DeviceId = cfgJson.DeviceId,
                    CaptureDt = Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                    FileName = kvp.Value[0],
                    FileType = "json",
                    FileContent = ImageHelper.ReadFile(kvp.Value[0])
                };
                Medium cfgJsonFile = new()
                {
                    Ymd = cfgJson.Ymd,
                    Hms = cfgJson.Hms,
                    MediumType = "cfg",
                    DeviceId = cfgJson.DeviceId,
                    CaptureDt = Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                    FileName = kvp.Value[1],
                    FileType = "json",
                    FileContent = ImageHelper.ReadFile(kvp.Value[1])
                };
                Medium irImgFile = new()
                {
                    Ymd = cfgJson.Ymd,
                    Hms = cfgJson.Hms,
                    MediumType = "ir",
                    DeviceId = cfgJson.DeviceId,
                    CaptureDt = Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                    FileName = kvp.Value[2],
                    FileType = "jpg",
                    FileContent = ImageHelper.ReadFile(kvp.Value[2])
                };
                Medium rgbImgFile = new()
                {
                    Ymd = cfgJson.Ymd,
                    Hms = cfgJson.Hms,
                    MediumType = "rgb",
                    DeviceId = cfgJson.DeviceId,
                    CaptureDt = Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                    FileName = kvp.Value[3],
                    FileType = "jpg",
                    FileContent = ImageHelper.ReadFile(kvp.Value[3])
                };
                Medium tempRawFile = new()
                {
                    Ymd = cfgJson.Ymd,
                    Hms = cfgJson.Hms,
                    MediumType = "raw",
                    DeviceId = cfgJson.DeviceId,
                    CaptureDt = Util.ToDateTime($"{cfgJson.Ymd}{cfgJson.Hms}"),
                    FileName = kvp.Value[4],
                    FileType = "csv",
                    FileContent = ImageHelper.ReadFile(kvp.Value[4])
                };

                data.Add(new Tuple<TemperatureJson, ConfigJson, Medium, Medium, Medium, Medium, Medium>(tempJson,
                                                                                                        cfgJson,
                                                                                                        tempJsonFile,
                                                                                                        cfgJsonFile,
                                                                                                        irImgFile,
                                                                                                        rgbImgFile,
                                                                                                        tempRawFile));
            }

            return data;
        }

        private void CreateData()
        {
            // Insert data to database
            using var repo = DeviceFixture.CreateRepo();
            foreach (var tuple in _data)
            {
                Device device = tuple.Item1.ToDevice();
                device.Cfgs.Add(tuple.Item2.ToCfg());
                device.Media.Add(tuple.Item3);
                device.Media.Add(tuple.Item4);
                device.Media.Add(tuple.Item5);
                device.Media.Add(tuple.Item6);
                device.Media.Add(tuple.Item7);
                Devices.Add(device);

                // Append each data to properties
                device.Frames.ToList().ForEach(x => Frames.Add(x));
                device.Rois.ToList().ForEach(x => Rois.Add(x));
                device.Boxes.ToList().ForEach(x => Boxes.Add(x));
                device.Cfgs.ToList().ForEach(x => Cfgs.Add(x));

                // Append media
                Media.Add(tuple.Item3);
                Media.Add(tuple.Item4);
                Media.Add(tuple.Item5);
                Media.Add(tuple.Item6);
                Media.Add(tuple.Item7);

                // Finally, save all data
                repo.CreateAlways(device);
            }
        }

        private void DeleteData()
        {
            using var frameRepo = FrameFixture.CreateRepo();
            using var roiRepo = RoiFixture.CreateRepo();
            using var boxRepo = BoxFixture.CreateRepo();
            using var mediumRepo = MediumFixture.CreateRepo();
            {
                foreach (var tuple in _data)
                {
                    Device device = tuple.Item1.ToDevice();
                    string deviceId = device.DeviceId;
                    string ymd = device.Frames.First().Ymd;
                    string hms = device.Frames.First().Hms;

                    frameRepo.Delete(ymd, hms, deviceId);
                    roiRepo.Delete(ymd, hms, deviceId);
                    boxRepo.Delete(ymd, hms, deviceId);
                    mediumRepo.Delete(ymd, hms, deviceId);
                }
            }
        }

        #endregion

        #region IDisposable Members

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing)
            {
                // TODO: dispose managed state (managed objects)
                DeleteData();

                DeviceFixture?.Dispose();
                FrameFixture?.Dispose();
                RoiFixture?.Dispose();
                BoxFixture?.Dispose();
                CfgFixture?.Dispose();
                MediumFixture?.Dispose();
            }

            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            DeviceFixture = null;
            FrameFixture = null;
            RoiFixture = null;
            BoxFixture = null;
            CfgFixture = null;
            MediumFixture = null;

            Devices = null;
            Frames = null;
            Rois = null;
            Boxes = null;
            Cfgs = null;
            Media = null;

            _disposed = true;
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
