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
using CSG.MI.TrMontrgSrv.Web.Services.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Services
{
    public class FrameCtrlDataService : DataServiceBase, IFrameCtrlDataService
    {
        public FrameCtrlDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<FrameCtrl> Get(string deviceId)
        {
            var uri = $"api/v{Version}/framectrl/{deviceId}";

            return await JsonDeserializerAsync<FrameCtrl>(uri);
        }

        public async Task<ImrCtrl> GetImrCtrl(string deviceId, int? roiId)
        {
            var uri = $"api/v{Version}/framectrl/imr/{deviceId}";

            return await JsonDeserializerAsync<ImrCtrl>(uri);
        }

        public async Task<List<FrameCtrl>> GetList()
        {
            var uri = $"api/v{Version}/framectrl";

            return await JsonDeserializerAsync<List<FrameCtrl>>(uri);
        }

        public async Task<FrameCtrl> Create(FrameCtrl frameCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(frameCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/framectrl";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
                return await JsonSerializer.DeserializeAsync<FrameCtrl>(await response.Content.ReadAsStreamAsync(),
                                                                        new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });

            return null;
        }

        public async Task<bool> Update(FrameCtrl frameCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(frameCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/framectrl";
            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(string deviceId)
        {
            var uri = $"api/v{Version}/framectrl/{deviceId}";
            var response = await _httpClient.DeleteAsync(uri);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
