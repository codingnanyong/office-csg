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
    public interface IRoiCtrlRepo : IDisposable
    {
        bool Exists(string deviceId, int roiId);

        RoiCtrl Get(string deviceId, int roiId);

        List<RoiCtrl> FindAll(string deviceId);

        RoiCtrl Create(RoiCtrl roiCtrl);

        Task<RoiCtrl> CreateAsync(RoiCtrl roiCtrl);

        RoiCtrl Update(RoiCtrl roiCtrl);

        Task<RoiCtrl> UpdateAsync(RoiCtrl roiCtrl);

        int Delete(string deviceId, int roiId);

        Task<int> DeleteAsync(string deviceId, int roiId);
    }
}
