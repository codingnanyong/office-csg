using CSG.MI.TrMontrgSrv.Dashboard.Services.Base.Interface;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard.Interface
{
    public interface IDashboardDataService 
    {
        Task<List<CurDevice>> GetDevicesByFactory(string factory);

        Task<CurDevice> GetDeviceById(string factory, string id);
    }
}
