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
    public class GrpKeyDataService : DataServiceBase, IGrpKeyDataService
    {
        public GrpKeyDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<GrpKey> Get(string group, string key)
        {
            var uri = $"api/v{Version}/grpkey/{group}/{key}";

            return await JsonDeserializerAsync<GrpKey>(uri);
        }

        public async Task<List<GrpKey>> GetList(string group = null)
        {
            string uri = $"api/v{Version}/grpkey";

            if (String.IsNullOrEmpty(group) == false)
                uri += $"/{group}";

            return await JsonDeserializerAsync<List<GrpKey>>(uri);
        }

        public async Task<GrpKey> Create(GrpKey grpKey)
        {
            var json = new StringContent(JsonSerializer.Serialize(grpKey), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/grpkey";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
                return await JsonSerializer.DeserializeAsync<GrpKey>(await response.Content.ReadAsStreamAsync(),
                                                                     new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });

            return null;
        }

        public async Task<bool> Update(GrpKey grpKey)
        {
            var json = new StringContent(JsonSerializer.Serialize(grpKey), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/grpkey";

            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(string group, string key)
        {
            var uri = $"api/v{Version}/grpkey/{group}/{key}";
            var response = await _httpClient.DeleteAsync(uri);

            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
