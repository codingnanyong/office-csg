using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IGrpKeyRepository : IGenericMapRepository<GrpKeyEntity, GrpKey>
    {
        bool Exists(string group, string key);

        GrpKey Get(string group, string key);

        ICollection<GrpKey> FindAll(string group);

        new ICollection<GrpKey> FindAll(Expression<Func<GrpKeyEntity, bool>> predicate);

        GrpKey Create(GrpKey grpKey);

        Task<GrpKey> CreateAsync(GrpKey grpKey);

        GrpKey Update(GrpKey grpKey);

        Task<GrpKey> UpdateAsync(GrpKey grpKey);

        int Delete(string group, string key);

        Task<int> DeleteAsync(string group, string key);
    }
}