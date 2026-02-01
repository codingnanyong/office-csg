using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Entities;

namespace CSG.MI.TrMontrgSrv.EF.Core.Repo
{
    public interface IGenericMapRepository<E, M> : IReadOnlyGenericMapRepository<E, M> where E : class
                                                                                       where M : class
    {
        #region Add/Delete/Update

        M Add(M model);

        Task<M> AddAsync(M model);

        int AddRange(IEnumerable<M> models);

        Task<int> AddRangeAsync(IEnumerable<M> models);

        int Delete(object key, params object[] keys);

        Task<int> DeleteAsync(object key, params object[] keys);


        M Update(M model, params object[] keys);

        Task<M> UpdateAsync(M model, params object[] keys);

        #endregion

        #region Save

        int Save();

        Task<int> SaveAsync();

        #endregion
    }
}