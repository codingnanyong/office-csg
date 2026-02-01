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
    public interface ICfgRepo : IDisposable
    {
        Cfg Get(int id);

        Task<Cfg> GetAsync(int id);

        Cfg GetLast(string deviceId);

        List<Cfg> GetLast(string deviceId, int last);

        List<Cfg> FindBy(string deviceId);

        Task<ICollection<Cfg>> FindByAsync(string deviceId);
    }
}
