using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IFrameCtrlDataService : IDataService, IImrCtrlDataService
    {
        Task<FrameCtrl> Get(string deviceId);

        Task<List<FrameCtrl>> GetList();

        Task<FrameCtrl> Create(FrameCtrl frameCtrl);

        Task<bool> Update(FrameCtrl frameCtrl);

        Task<bool> Delete(string deviceId);
    }
}
