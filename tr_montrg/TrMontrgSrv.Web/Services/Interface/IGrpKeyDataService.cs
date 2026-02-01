using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IGrpKeyDataService : IDataService
    {
        Task<GrpKey> Get(string group, string key);

        Task<List<GrpKey>> GetList(string group = null);

        Task<GrpKey> Create(GrpKey grpKey);

        Task<bool> Update(GrpKey grpKey);

        Task<bool> Delete(string group, string key);
    }
}
