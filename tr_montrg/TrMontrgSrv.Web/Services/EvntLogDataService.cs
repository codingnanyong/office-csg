
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
    public class EvntLogDataService : DataServiceBase, IEvntLogDataService
    {
        public EvntLogDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<bool> Exists(string deviceId, DateTime dt)
        {
            string uri = $"api/v{Version}/evntlog/exist/{deviceId}/{dt.ToYmdHms()}";

            return await JsonDeserializerAsync<bool>(uri);
        }

        public async Task<EvntLog> Get(int id)
        {
            string uri = $"api/v{Version}/evntlog/{id}";

            return await JsonDeserializerAsync<EvntLog>(uri);
        }

        public async Task<List<EvntLog>> GetList(string deviceId, string start = null, string end = null)
        {
            string uri = $"api/v{Version}/evntlog/{deviceId}";

            if (String.IsNullOrEmpty(start) == false && String.IsNullOrEmpty(end) == false)
                uri += $"?start={start}&end={end}";

            return await JsonDeserializerAsync<List<EvntLog>>(uri);
        }

        public async Task<List<EvntLog>> GetList(string deviceId, string evntLevel, string start, string end)
        {
            string uri = $"api/v{Version}/evntlog/{deviceId}/{evntLevel}?start={start}&end={end}";

            return await JsonDeserializerAsync<List<EvntLog>>(uri);
        }

        public async Task<List<EvntLog>> GetListByPlant(string plantId, string start = null, string end = null)
        {
            string uri = $"api/v{Version}/evntlog?plantId={plantId}";

            if (String.IsNullOrEmpty(start) == false && String.IsNullOrEmpty(end) == false)
                uri += $"&start={start}&end={end}";

            return await JsonDeserializerAsync<List<EvntLog>>(uri);
        }

        public async Task<List<EvntLog>> GetLatest(string plantId, string deviceId = null, bool excludingInfoLevel = false)
        {
            string uri = $"api/v{Version}/evntlog/latest/{plantId}";

            if (String.IsNullOrEmpty(deviceId) == false)
                uri += $"/{deviceId}";

            if (excludingInfoLevel)
                uri += $"?excludingInfoLevel";

            return await JsonDeserializerAsync<List<EvntLog>>(uri);
        }

        public async Task<EvntLog> Create(EvntLog evntLog)
        {
            var json = new StringContent(JsonSerializer.Serialize(evntLog), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/evntlog";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
            {
                var result = await JsonSerializer.DeserializeAsync<EvntLog>(await response.Content.ReadAsStreamAsync(),
                                                                            new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });
                return result;
            }

            return null;
        }

        public async Task<bool> Update(EvntLog evntLog)
        {
            var json = new StringContent(JsonSerializer.Serialize(evntLog), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/evntlog";

            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(int id)
        {
            var uri = $"api/v{Version}/evntlog/{id}";

            var response = await _httpClient.DeleteAsync(uri);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
