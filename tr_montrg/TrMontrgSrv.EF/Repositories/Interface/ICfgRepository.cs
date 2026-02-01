using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface ICfgRepository : IGenericMapRepository<CfgEntity, Cfg>
    {
        Cfg Get(int id);

        Task<Cfg> GetAsync(int id);

        Cfg GetLast(string deviceId);

        ICollection<Cfg> GetLast(string deviceId, int last);

        ICollection<Cfg> FindBy(string deviceId);

        Task<ICollection<Cfg>> FindByAsync(string deviceId);
    }
}
