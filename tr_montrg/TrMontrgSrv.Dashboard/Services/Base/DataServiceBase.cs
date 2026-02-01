using CSG.MI.TrMontrgSrv.Dashboard.Services.Base.Interface;
using System.Net;
using System.Text.Json;


namespace CSG.MI.TrMontrgSrv.Dashboard.Services.Base
{
    public abstract class DataServiceBase : IDataService
    {
        #region Fields

        protected readonly HttpClient _httpClient;

        #endregion

        #region Constructors

        public DataServiceBase(HttpClient httpClient, string host, float version)
        {

            _httpClient = httpClient;
            Host = host;
            Version = version;
        }

        #endregion

        #region Properties

        public string Host { get; }

        public float Version { get; }

        public HttpClient HttpClient { get => _httpClient; }

        #endregion

        #region Public Methods

        public async Task<T> JsonDeserializerAsync<T>(string uri)
        {
            try
            {
                System.Diagnostics.Debug.WriteLine($"HTTP GET {uri}");

                using var response = await _httpClient.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead);
                var statusCode = response.StatusCode;

                System.Diagnostics.Debug.WriteLine($"STATUS CODE: {statusCode}");

                if (statusCode != HttpStatusCode.OK && statusCode != HttpStatusCode.Created)
                    return default!;

                using var stream = await response.Content.ReadAsStreamAsync();

                return (await JsonSerializer.DeserializeAsync<T>(stream, new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                })) ?? default!;
            }
            catch (Exception ex)
            {
                // TODO: log error
                System.Diagnostics.Debug.WriteLine($"ERROR: {ex.Message}");
                return default!;
            }
        }

        #endregion
    }
}
