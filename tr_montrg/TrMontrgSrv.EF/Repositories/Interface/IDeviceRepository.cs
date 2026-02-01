using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IDeviceRepository : IGenericMapRepository<DeviceEntity, Device>
    {
        bool Exists(string deviceId);

        Device Get(string deviceId);

        new ICollection<Device> GetAll();

        new ICollection<Device> FindAll(Expression<Func<DeviceEntity, bool>> predicate);

        ICollection<Device> FindBy(string plantId, string locationId = null);

        Task<ICollection<Device>> FindByAsync(string plantId, string locationId = null);

        bool CreateAlways(Device device);

        Device Create(Device device);

        Task<Device> CreateAsync(Device device);

        Device Update(Device device);

        Task<Device> UpdateAsync(Device device);

        int Delete(string deviceId);

        Task<int> DeleteAsync(string deviceId);

    }
}
