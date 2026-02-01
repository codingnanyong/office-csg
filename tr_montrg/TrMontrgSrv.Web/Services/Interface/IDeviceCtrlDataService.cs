using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IDeviceCtrlDataService : IDataService
    {
        Task<DeviceCtrl> Get(string deviceId);

        Task<List<DeviceCtrl>> GetList();

        Task<DeviceCtrl> Create(DeviceCtrl deviceCtrl);

        Task<bool> Update(DeviceCtrl deviceCtrl);

        Task<bool> Delete(string deviceId);
    }
}
