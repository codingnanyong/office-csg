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
    public class MailingAddrDataService : DataServiceBase, IMailingAddrDataService
    {
        public MailingAddrDataService(HttpClient httpClient) : base(httpClient)
        {
        }

        public async Task<MailingAddr> Get(string plantId, string email)
        {
            var uri = $"api/v{Version}/mailingaddr/{plantId}/{email}";

            return await JsonDeserializerAsync<MailingAddr>(uri);
        }

        public async Task<List<MailingAddr>> GetList(string plantId)
        {
            var uri = $"api/v{Version}/mailingaddr/{plantId}";

            return await JsonDeserializerAsync<List<MailingAddr>>(uri);
        }

        public async Task<List<MailingAddr>> GetActiveList()
        {
            var uri = $"api/v{Version}/mailingaddr";

            return await JsonDeserializerAsync<List<MailingAddr>>(uri);
        }

        public async Task<MailingAddr> Create(MailingAddr mailingAddr)
        {
            var json = new StringContent(JsonSerializer.Serialize(mailingAddr), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/mailingaddr";

            var response = await _httpClient.PostAsync(uri, json);
            if (response.IsSuccessStatusCode)
                return await JsonSerializer.DeserializeAsync<MailingAddr>(await response.Content.ReadAsStreamAsync(),
                                                                          new JsonSerializerOptions() { PropertyNameCaseInsensitive = true });

            return null;
        }

        public async Task<bool> Update(MailingAddr mailingAddr)
        {
            var json = new StringContent(JsonSerializer.Serialize(mailingAddr), Encoding.UTF8, MediaTypeNames.Application.Json);
            var uri = $"api/v{Version}/mailingaddr";

            var response = await _httpClient.PutAsync(uri, json);
            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }

        public async Task<bool> Delete(string group, string key)
        {
            var uri = $"api/v{Version}/mailingaddr/{group}/{key}";
            var response = await _httpClient.DeleteAsync(uri);

            return (response.StatusCode == System.Net.HttpStatusCode.NoContent);
        }
    }
}
