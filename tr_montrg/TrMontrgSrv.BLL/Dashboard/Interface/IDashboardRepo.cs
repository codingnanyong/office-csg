using CSG.MI.TrMontrgSrv.Model.Dashboard;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.BLL.Dashboard.Interface
{
    public interface IDashboardRepo : IDisposable
    {
        ICollection<CurDevice> FindDevices();

        Task<ICollection<CurDevice>> FindDevicesAsync();

        ICollection<CurDevice> FindDeviceByFactory(string factory);

        Task<ICollection<CurDevice>> FindDeviceByFactoryAsync(string factory);

        CurDevice FindDeviceById(string factory, string devuce_id);

        Task<CurDevice> FindDeviceByIdAsync(string factory, string devuce_id);
    }
}
