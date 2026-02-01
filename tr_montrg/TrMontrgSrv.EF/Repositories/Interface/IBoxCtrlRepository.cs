using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IBoxCtrlRepository : IGenericMapRepository<BoxCtrlEntity, BoxCtrl>
    {
        bool Exists(string deviceId);

        BoxCtrl Get(string deviceId);

        new ICollection<BoxCtrl> GetAll();

        new ICollection<BoxCtrl> FindAll(Expression<Func<BoxCtrlEntity, bool>> predicate);

        BoxCtrl Create(BoxCtrl frameCtrl);

        Task<BoxCtrl> CreateAsync(BoxCtrl frameCtrl);

        BoxCtrl Update(BoxCtrl frameCtrl);

        Task<BoxCtrl> UpdateAsync(BoxCtrl frameCtrl);

        int Delete(string deviceId);

        Task<int> DeleteAsync(string deviceId);
    }
}