using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IDeviceCtrlRepository : IGenericMapRepository<DeviceCtrlEntity, DeviceCtrl>
    {
        bool Exists(string deviceId);

        DeviceCtrl Get(string deviceId);

        new ICollection<DeviceCtrl> GetAll();

        new ICollection<DeviceCtrl> FindAll(Expression<Func<DeviceCtrlEntity, bool>> predicate);

        DeviceCtrl Create(DeviceCtrl deviceCtrl);

        Task<DeviceCtrl> CreateAsync(DeviceCtrl deviceCtrl);

        DeviceCtrl Update(DeviceCtrl deviceCtrl);

        Task<DeviceCtrl> UpdateAsync(DeviceCtrl deviceCtrl);

        int Delete(string deviceId);

        Task<int> DeleteAsync(string deviceId);
    }
}