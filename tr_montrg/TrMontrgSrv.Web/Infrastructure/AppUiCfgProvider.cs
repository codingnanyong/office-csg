using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;

namespace CSG.MI.TrMontrgSrv.Web.Infrastructure
{
    /// <summary>
    ///
    /// </summary>
    public interface IAppUiCfgProvider
    {
        Dictionary<string, Dictionary<string, string>> GetAllDeviceUiCfgs();

        Dictionary<string, string> GetUiCfgs(string devicesId);
    }

    /// <summary>
    ///
    /// </summary>
    public class AppUiCfgProvider: IAppUiCfgProvider
    {
        private readonly IHostEnvironment _env;
        private const string APP_UI_CFG_FILENAME = "app_ui_cfg.json";

        public AppUiCfgProvider(IHostEnvironment env)
        {
            _env = env;
        }

        public Dictionary<string, Dictionary<string, string>> GetAllDeviceUiCfgs()
        {
            var cfgs = new Dictionary<string, Dictionary<string, string>>();

            var rootPath = _env.ContentRootPath;
            var fullPath = Path.Combine(rootPath, APP_UI_CFG_FILENAME);

            using (var reader = new StreamReader(fullPath))
            {
                var jsonString = reader.ReadToEnd();
                var jsonDoc = JsonDocument.Parse(jsonString);
                var devicesObj = jsonDoc.RootElement.GetProperty("devices").ToString();
                cfgs = JsonConvert.DeserializeObject<Dictionary<string, Dictionary<string, string>>>(devicesObj);
            }

            return cfgs;
        }

        public Dictionary<string, string> GetUiCfgs(string devicesId)
        {
            var cfgs = new Dictionary<string, string>();
            var success = GetAllDeviceUiCfgs().TryGetValue(devicesId, out cfgs);

            return cfgs;
        }
    }
}
