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
    public class FrameDataService : DataServiceBase, IFrameDataService
    {
        public FrameDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<List<Frame>> GetList(string deviceId, string start, string end)
        {
            var uri = $"api/v{Version}/frame/{deviceId}/?start={start}&end={end}";
            //return await _httpClient.GetFromJsonAsync<List<Frame>>(uri);

            return await JsonDeserializerAsync<List<Frame>>(uri);
        }
    }
}
