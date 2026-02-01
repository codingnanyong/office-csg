
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using CSG.MI.TrMontrgSrv.Model.Interface;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Services
{
    public class RoiCtrlDataService : DataServiceBase, IRoiCtrlDataService
    {
        public RoiCtrlDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<RoiCtrl> Get(string deviceId, int roiId)
        {
            var uri = $"api/v{Version}/roictrl/{deviceId}/{roiId}";

            return await JsonDeserializerAsync<RoiCtrl>(uri);
        }

        public async Task<ImrCtrl> GetImrCtrl(string deviceId, int? roiId)
        {
            var uri = $"api/v{Version}/roictrl/imr/{deviceId}/{roiId}";

            return await JsonDeserializerAsync<ImrCtrl>(uri);
        }

        public async Task<List<RoiCtrl>> GetList(string deviceId)
        {
            var uri = $"api/v{Version}/roictrl/{deviceId}";

            return await JsonDeserializerAsync<List<RoiCtrl>>(uri);
        }

        public async Task<RoiCtrl> Create(RoiCtrl roiCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(roiCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/roictrl";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
                return await JsonSerializer.DeserializeAsync<RoiCtrl>(await response.Content.ReadAsStreamAsync(),
                                                                      new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });

            return null;
        }

        public async Task<bool> Update(RoiCtrl roiCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(roiCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/roictrl";

            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(string deviceId, int roiId)
        {
            var uri = $"api/v{Version}/roictrl/{deviceId}/{roiId}";
            var response = await _httpClient.DeleteAsync(uri);

            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
