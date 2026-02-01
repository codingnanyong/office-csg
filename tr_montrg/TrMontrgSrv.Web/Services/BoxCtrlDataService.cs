
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Mime;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Services
{
    public class BoxCtrlDataService : DataServiceBase, IBoxCtrlDataService
    {
        public BoxCtrlDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<BoxCtrl> Get(string deviceId)
        {
            var uri = $"api/v{Version}/boxctrl/{deviceId}";

            return await JsonDeserializerAsync<BoxCtrl>(uri);
        }

        public async Task<List<BoxCtrl>> GetList()
        {
            var uri = $"api/v{Version}/boxctrl";

            return await JsonDeserializerAsync<List<BoxCtrl>>(uri);
        }

        public async Task<BoxCtrl> Create(BoxCtrl boxCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(boxCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/boxctrl";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
                return await JsonSerializer.DeserializeAsync<BoxCtrl>(await response.Content.ReadAsStreamAsync(),
                                                                      new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });

            return null;
        }

        public async Task<bool> Update(BoxCtrl boxCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(boxCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/boxctrl";

            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(string deviceId)
        {
            var uri = $"api/v{Version}/boxctrl/{deviceId}";
            var response = await _httpClient.DeleteAsync(uri);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
