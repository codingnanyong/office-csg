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
    public interface IFrameCtrlRepo : IDisposable
    {
        bool Exists(string deviceId);

        FrameCtrl Get(string deviceId);

        List<FrameCtrl> GetAll();

        FrameCtrl Create(FrameCtrl deviceCtrl);

        Task<FrameCtrl> CreateAsync(FrameCtrl deviceCtrl);

        FrameCtrl Update(FrameCtrl deviceCtrl);

        Task<FrameCtrl> UpdateAsync(FrameCtrl deviceCtrl);

        int Delete(string deviceId);

        Task<int> DeleteAsync(string deviceId);
    }
}
