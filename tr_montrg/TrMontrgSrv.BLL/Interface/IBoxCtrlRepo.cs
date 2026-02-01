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
    public interface IBoxCtrlRepo : IDisposable
    {
        bool Exists(string deviceId);

        BoxCtrl Get(string deviceId);

        List<BoxCtrl> GetAll();

        BoxCtrl Create(BoxCtrl boxCtrl);

        Task<BoxCtrl> CreateAsync(BoxCtrl boxCtrl);

        BoxCtrl Update(BoxCtrl boxCtrl);

        Task<BoxCtrl> UpdateAsync(BoxCtrl boxCtrl);

        int Delete(string deviceId);

        Task<int> DeleteAsync(string deviceId);
    }
}
