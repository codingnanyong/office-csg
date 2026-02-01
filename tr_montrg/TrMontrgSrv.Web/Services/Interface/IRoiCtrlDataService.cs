using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IRoiCtrlDataService : IDataService, IImrCtrlDataService
    {
        Task<RoiCtrl> Get(string deviceId, int roiId);

        Task<List<RoiCtrl>> GetList(string deviceId);

        Task<RoiCtrl> Create(RoiCtrl roiCtrl);

        Task<bool> Update(RoiCtrl roiCtrl);

        Task<bool> Delete(string deviceId, int roiId);
    }
}
