using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Web.Infrastructure;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Services
{
    public abstract class DataServiceBase : IDataService
    {
        #region Fields

        protected readonly HttpClient _httpClient;

        #endregion

        #region Constructors

        public DataServiceBase(HttpClient httpClient)
        {
            _httpClient = httpClient;
        }

        #endregion

        #region Properties

        public string Host { get; } = AppSettingsProvider.WebApiHostUri;

        public int Version { get; } = AppSettingsProvider.WebApiVersion;

        public HttpClient HttpClient { get => _httpClient; }

        #endregion

        #region Public Methods

        public async Task<T> JsonDeserializerAsync<T>(string uri)
        {
            try
            {
                // https://stackoverflow.com/questions/30163316/httpclient-getstreamasync-and-http-status-codes

                var response = await _httpClient.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead);
                var statusCode = response.StatusCode;

                System.Diagnostics.Debug.WriteLine($"HTTP GET {uri}");
                System.Diagnostics.Debug.WriteLine($"STATUS CODE: {statusCode}");

                //// In case of PUT, DELETE
                //if (statusCode == HttpStatusCode.NoContent)
                //    return (T)Activator.CreateInstance(typeof(T));

                if (statusCode != HttpStatusCode.OK && statusCode != HttpStatusCode.Created)
                    return default;

                using var stream = await response.Content.ReadAsStreamAsync();
                //using var stream = CopyAndClose(await response.Content.ReadAsStreamAsync());
                //using var stream = CopyAndClose(await _httpClient.GetStreamAsync(uri));
                return await JsonSerializer.DeserializeAsync<T>(stream,
                                                                new JsonSerializerOptions()
                                                                {
                                                                    PropertyNameCaseInsensitive = true
                                                                });
            }
            catch (Exception ex)
            {
                // TODO: log error
                System.Diagnostics.Debug.WriteLine($"ERROR: {ex.Message}");
                return default;
            }
        }

        #endregion

        #region Private Methods

        //private static Stream CopyAndClose(Stream stream)
        //{
        //    const int readSize = 256;
        //    byte[] buffer = new byte[readSize];
        //    MemoryStream ms = new();

        //    int count = stream.Read(buffer, 0, readSize);
        //    while (count > 0)
        //    {
        //        ms.Write(buffer, 0, count);
        //        count = stream.Read(buffer, 0, readSize);
        //    }
        //    ms.Position = 0;
        //    stream.Close();

        //    return ms;
        //}

        #endregion
    }
}
