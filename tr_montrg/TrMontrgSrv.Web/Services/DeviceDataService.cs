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
    public class DeviceDataService: DataServiceBase, IDeviceDataService
    {
        public DeviceDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<Device> Get(string deviceId)
        {
            var uri = $"api/v{Version}/device/{deviceId}";

            return await JsonDeserializerAsync<Device>(uri);
        }

        public async Task<List<Device>> GetList(string plantId = null)
        {
            var uri = String.IsNullOrEmpty(plantId) ? $"api/v{Version}/device" : $"api/v{Version}/device?plantId={plantId}";

            return await JsonDeserializerAsync<List<Device>>(uri);
        }

        public async Task<Device> Create(Device device)
        {
            var json = new StringContent(JsonSerializer.Serialize(device), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/device";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
                return await JsonSerializer.DeserializeAsync<Device>(await response.Content.ReadAsStreamAsync(),
                                                                     new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });

            return null;
        }

        public async Task<bool> Update(Device device)
        {
            var json = new StringContent(JsonSerializer.Serialize(device), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/device";

            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(string deviceId)
        {
            var uri = $"api/v{Version}/device/{deviceId}";
            var response = await _httpClient.DeleteAsync(uri);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
