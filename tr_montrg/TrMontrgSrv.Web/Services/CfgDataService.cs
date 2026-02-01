using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Services
{
    public class CfgDataService : DataServiceBase, ICfgDataService
    {
        public CfgDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<Cfg> Get(string deviceId)
        {
            var uri = $"api/v{Version}/cfg/{deviceId}/last";
            //return await _httpClient.GetFromJsonAsync<Cfg>(uri);

            return await JsonDeserializerAsync<Cfg>(uri);
        }

        public async Task<Dictionary<int, int[]>> GetCfgRois(string deviceId)
        {
            //Dictionary<int, int[]> dic = new();
            //var cfg = await GetCfg(deviceId);
            //foreach (var kvp in cfg.CfgJson.RoiCoord)
            //    dic.Add(Convert.ToInt32(kvp.Key), kvp.Value);
            //return dic;

            var uri = $"{Host}/api/v{Version}/cfg/{deviceId}/roi";
            //return await _httpClient.GetFromJsonAsync<Dictionary<int, int[]>>(uri);

            return await JsonDeserializerAsync<Dictionary<int, int[]>>(uri);
        }
    }
}
