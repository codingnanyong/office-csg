using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IDeviceDataService : IDataService
    {
        Task<Device> Get(string deviceId);

        Task<List<Device>> GetList(string plantId = null);

        Task<Device> Create(Device device);

        Task<bool> Update(Device device);

        Task<bool> Delete(string deviceId);
    }
}
