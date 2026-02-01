using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Dashboard.Interface
{
    public interface IDashboardRepository : IGenericMapRepository<CurDeviceEntity, CurDevice>
    {
        ICollection<CurDevice> GetDevices();

        Task<ICollection<CurDevice>> GetDevicesAsync();

        ICollection<CurDevice> GetByFactory(string factory);

        Task<ICollection<CurDevice>> GetByFactoryAsync(string factory);

        CurDevice GetDeviceById(string factory, string devuce_id);

        Task<CurDevice> GetDeviceByIdAsync(string factory, string devuce_id);
    }
}