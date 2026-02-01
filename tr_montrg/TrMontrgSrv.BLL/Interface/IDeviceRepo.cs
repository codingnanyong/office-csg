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
    public interface IDeviceRepo : IDisposable
    {
        bool Exists(string deviceId);

        Device Get(string deviceId);

        List<Device> GetAll();

        Device GetSnap(string ymd, string hms, string deviceId);

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
