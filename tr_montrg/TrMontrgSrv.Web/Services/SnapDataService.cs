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
    public class SnapDataService : DataServiceBase, ISnapDataService
    {
        public SnapDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<Device> Get(string deviceId, string ymdhms)
        {
            var uri = $"api/v{Version}/snap/{deviceId}/{ymdhms}";
            //return await _httpClient.GetFromJsonAsync<Device>(uri);

            var device = await JsonDeserializerAsync<Device>(uri);

            return device;
        }
    }
}
