using CSG.MI.TrMontrgSrv.Dashboard.Services.Base;
using CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
using CSG.MI.TrMontrgSrv.Dashboard.Infrastructure;

namespace CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard
{
    public class DashboardDataService : DataServiceBase, IDashboardDataService
    {
        #region Constructor

        public DashboardDataService(HttpClient httpClient) : base(httpClient, AppSettingProvider.WebApiHostUri, AppSettingProvider.WebApiVersion)
        {
        }

        #endregion

        #region Public Methods

        public async Task<List<CurDevice>> GetDevicesByFactory(string factory)
        {
            if (String.IsNullOrEmpty(factory))
            {
                throw new ArgumentException("Factory cannot be empty or null.", nameof(factory));
            }

            var uri = $"api/v{Version}/Dashboard/{factory}/async";

            return await JsonDeserializerAsync<List<CurDevice>>(uri);
        }

        public async Task<CurDevice> GetDeviceById(string factory, string id)
        {
            if (String.IsNullOrEmpty(factory) || String.IsNullOrEmpty(id))
            {
                throw new ArgumentException("Factory and id cannot be empty or null.");
            }

            var uri = $"api/v{Version}/Dashboard/{factory}/{id}/async";

            return await JsonDeserializerAsync<CurDevice>(uri);
        }

        #endregion
    }
}