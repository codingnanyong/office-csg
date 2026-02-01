using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Services
{
    public class ImrDataService : DataServiceBase, IImrDataService
    {
        public ImrDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<List<ImrData>> GetFrameImrData(string deviceId, string start, string end)
        {
            var uri = $"api/v{Version}/imr/{deviceId}/frame?start={start}&end={end}";
            //return await _httpClient.GetFromJsonAsync<List<ImrChartInfo>>(uri);

            return await JsonDeserializerAsync<List<ImrData>>(uri);
        }

        public async Task<List<ImrData>> GetRoiImrData(int roiId, string deviceId, string start, string end)
        {
            var uri = $"api/v{Version}/imr/{deviceId}/roi/{roiId}?start={start}&end={end}";
            //return await _httpClient.GetFromJsonAsync<List<ImrChartInfo>>(uri);

            return await JsonDeserializerAsync<List<ImrData>>(uri);
        }
    }
}
