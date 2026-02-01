using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IFrameCtrlRepository : IGenericMapRepository<FrameCtrlEntity, FrameCtrl>
    {
        bool Exists(string deviceId);

        FrameCtrl Get(string deviceId);

        new ICollection<FrameCtrl> GetAll();

        new ICollection<FrameCtrl> FindAll(Expression<Func<FrameCtrlEntity, bool>> predicate);

        FrameCtrl Create(FrameCtrl frameCtrl);

        Task<FrameCtrl> CreateAsync(FrameCtrl frameCtrl);

        FrameCtrl Update(FrameCtrl frameCtrl);

        Task<FrameCtrl> UpdateAsync(FrameCtrl frameCtrl);

        int Delete(string deviceId);

        Task<int> DeleteAsync(string deviceId);
    }
}