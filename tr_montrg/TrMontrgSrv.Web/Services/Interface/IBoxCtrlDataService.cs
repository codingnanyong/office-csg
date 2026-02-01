using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IBoxCtrlDataService : IDataService
    {
        Task<BoxCtrl> Get(string deviceId);

        Task<List<BoxCtrl>> GetList();

        Task<BoxCtrl> Create(BoxCtrl boxCtrl);

        Task<bool> Update(BoxCtrl boxCtrl);

        Task<bool> Delete(string deviceId);
    }
}
