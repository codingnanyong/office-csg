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
    public class DeviceCtrlDataService : DataServiceBase, IDeviceCtrlDataService
    {
        public DeviceCtrlDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<DeviceCtrl> Get(string deviceId)
        {
            var uri = $"api/v{Version}/devicectrl/{deviceId}";

            return await JsonDeserializerAsync<DeviceCtrl>(uri);
        }

        public async Task<List<DeviceCtrl>> GetList()
        {
            var uri = $"api/v{Version}/devicectrl";

            return await JsonDeserializerAsync<List<DeviceCtrl>>(uri);
        }

        public async Task<DeviceCtrl> Create(DeviceCtrl deviceCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(deviceCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/devicectrl";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
                return await JsonSerializer.DeserializeAsync<DeviceCtrl>(await response.Content.ReadAsStreamAsync(),
                                                                         new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });

            return null;
        }

        public async Task<bool> Update(DeviceCtrl deviceCtrl)
        {
            var json = new StringContent(JsonSerializer.Serialize(deviceCtrl), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/devicectrl";

            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(string deviceId)
        {
            var uri = $"api/v{Version}/devicectrl/{deviceId}";

            var response = await _httpClient.DeleteAsync(uri);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
