using CSG.MI.DTO.Feedback;
using CSG.MI.TrMontrgSrv.Dashboard.Infrastructure;
using CSG.MI.TrMontrgSrv.Dashboard.Services.Base;
using CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard.Interface;
using System.Text;
using System.Text.Json;

namespace CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard
{
    public class FdwDataService : DataServiceBase, IFdwDataService
    {
        #region Constructor

        public FdwDataService(HttpClient httpClient) : base(httpClient, AppSettingProvider.FDWApiHostUri, AppSettingProvider.FDWApiVersion)
        {
        }

        #endregion

        #region Public Methods

        public async Task<ICollection<Category>> GetCategories(string lang)
        {
            if (String.IsNullOrEmpty(lang))
            {
                throw new ArgumentException("Language cannot be empty or null.", nameof(lang));
            }

            var uri = $"api/v{Version}/category/async?lang={lang}";

            try
            {
                return await JsonDeserializerAsync<ICollection<Category>>(uri);
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task SendRequest(Feedback feedback)
        {
            if (feedback == null)
            {
                throw new ArgumentNullException(nameof(feedback));
            }

            var uri = $"api/v{Version}/feedback";
            try
            {
                var json = JsonSerializer.Serialize(feedback);
                var content = new StringContent(json, Encoding.UTF8, "application/json");
                var response = await _httpClient.PostAsync(uri, content);
                response.EnsureSuccessStatusCode();

            }
            catch (Exception e) 
            {
                throw new Exception("Failed to post feedback.", e);
            }
        }

        #endregion
    }
}
