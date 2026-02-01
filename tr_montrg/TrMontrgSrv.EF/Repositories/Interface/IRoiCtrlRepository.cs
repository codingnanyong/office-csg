using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IRoiCtrlRepository : IGenericMapRepository<RoiCtrlEntity, RoiCtrl>
    {
        bool Exists(string deviceId, int roiId);

        RoiCtrl Get(string deviceId, int roiId);

        ICollection<RoiCtrl> FindAll(string deviceId);

        new ICollection<RoiCtrl> FindAll(Expression<Func<RoiCtrlEntity, bool>> predicate);

        RoiCtrl Create(RoiCtrl roiCtrl);

        Task<RoiCtrl> CreateAsync(RoiCtrl roiCtrl);

        RoiCtrl Update(RoiCtrl roiCtrl);

        Task<RoiCtrl> UpdateAsync(RoiCtrl roiCtrl);

        int Delete(string deviceId, int roiId);

        Task<int> DeleteAsync(string deviceId, int roiId);
    }
}