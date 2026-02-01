using CSG.MI.TrMontrgSrv.AutoBtg.Generator.Interface;
using CSG.MI.TrMontrgSrv.AutoBtg.Inspector.Interface;
using CSG.MI.TrMontrgSrv.AutoBtg.PingCore.Interface;
using CSG.MI.TrMontrgSrv.Model.Inspection;
using Newtonsoft.Json;
using System.Net.Http.Headers;
using System.Net.NetworkInformation;

namespace CSG.MI.TrMontrgSrv.AutoBtg.Inspector
{
    public class IoTInspector : IIoTInspector<InspcDevice>
    {
        #region Field

        private static readonly string ApiUrl = Environment.GetEnvironmentVariable("IOT_API_URL") ?? "http://localhost:7071/api/v1/Inspection";

        private readonly IPingWrapper _pingWrapper;

        public IBatchGenerator<InspcDevice> _batch;

        #endregion

        #region Constructor

        public IoTInspector(IBatchGenerator<InspcDevice> batch, IPingWrapper pingWrapper)
        {
            _pingWrapper = pingWrapper;
            _batch = batch;
            HttpClient = new HttpClient();
        }

        #endregion

        #region Properties

        public HttpClient HttpClient { get; set; }

        #endregion

        #region Public Methods

        /// <summary>
        /// Retrieves a list of devices to be inspected using the API.
        /// </summary>
        /// <returns>A list of <see cref="CSG.MI.TrMontrgSrv.Model.Dashboard.CurDevice"/></returns>
        public async Task<ICollection<InspcDevice>> GetDatas()
        {
            ICollection<InspcDevice>? resultDevices = new List<InspcDevice>();

            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    httpClient.Timeout = TimeSpan.FromSeconds(30);

                    HttpResponseMessage response = await httpClient.GetAsync(ApiUrl);

                    if (response.IsSuccessStatusCode)
                    {
                        string responseBody = await response.Content.ReadAsStringAsync();
                        resultDevices = JsonConvert.DeserializeObject<ICollection<InspcDevice>>(responseBody);
                    }
                    else
                    {
                        StaticLogger.Logger.LogError($"API request failed with status code: {response.StatusCode}");
                    }
                }
            }
            catch (HttpRequestException ex)
            {
                StaticLogger.Logger.LogError($"An error occurred while calling API: {ex.Message}");
            }
            catch (JsonException ex)
            {
                StaticLogger.Logger.LogError($"An error occurred while deserializing API response: {ex.Message}");
            }
            catch (Exception ex)
            {
                StaticLogger.Logger.LogError($"An unexpected error occurred: {ex.Message}");
            }

            return resultDevices!;
        }

        /// <summary>
        /// Check device network connection. -> If normal, batch file operation / If error, send email.
        /// </summary>
        /// <param name="iots">list of devices to be inspected</param>
        public void InspectionData(ICollection<InspcDevice> iots)
        {
            if (iots == null)
            {
                StaticLogger.Logger.LogInfo("All Device is in normal operation.");
                return;
            }
            foreach (var iot in iots)
            {
                if (PingTest(iot))
                {
                    _batch.Run(iot);
                }
                else
                {
                    /*TODO...No data today & device disconnected 
                     * Add to Action.
                     */
                    StaticLogger.Logger.LogInfo($"{iot.DeviceId} is Disconnected. Request an device inspection.");
                    return;
                }
            }
        }

        /// <summary>
        /// Device ping test
        /// </summary>
        /// <param name="iot">devices to be inspected</param>
        /// <returns></returns>
        public bool PingTest(InspcDevice iot)
        {
            PingReply reply = _pingWrapper.Send(iot.IpAddress);
            return reply?.Status == IPStatus.Success;
        }

        #endregion
    }
}
