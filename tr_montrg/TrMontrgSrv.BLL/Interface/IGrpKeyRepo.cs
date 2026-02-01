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
    public interface IGrpKeyRepo : IDisposable
    {
        bool Exists(string group, string key);

        GrpKey Get(string group, string key);

        List<GrpKey> GetAll();

        List<GrpKey> FindAll(string group);

        GrpKey Create(GrpKey grpKey);

        Task<GrpKey> CreateAsync(GrpKey grpKey);

        GrpKey Update(GrpKey grpKey);

        Task<GrpKey> UpdateAsync(GrpKey grpKey);

        int Delete(string group, string key);

        Task<int> DeleteAsync(string group, string key);
    }
}
