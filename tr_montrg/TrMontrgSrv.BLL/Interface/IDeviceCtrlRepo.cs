using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL.Interface
{
    /// <summary>
    ///
    /// </summary>
    public interface IDeviceCtrlRepo : IDisposable
    {
        bool Exists(string deviceId);

        DeviceCtrl Get(string deviceId);

        List<DeviceCtrl> GetAll();

        DeviceCtrl Create(DeviceCtrl deviceCtrl);

        Task<DeviceCtrl> CreateAsync(DeviceCtrl deviceCtrl);

        DeviceCtrl Update(DeviceCtrl deviceCtrl);

        Task<DeviceCtrl> UpdateAsync(DeviceCtrl deviceCtrl);

        int Delete(string deviceId);

        Task<int> DeleteAsync(string deviceId);
    }
}
