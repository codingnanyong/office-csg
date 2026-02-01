using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class RoiCtrlRepository : GenericMapRepository<RoiCtrlEntity, RoiCtrl>, IRoiCtrlRepository, IDisposable
    {
        #region Constructors

        public RoiCtrlRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(string deviceId, int roiId)
        {
            return Context.Set<RoiCtrlEntity>().Any(x => x.DeviceId == deviceId && x.RoiId == roiId);
        }

        public RoiCtrl Get(string deviceId, int roiId)
        {
            var entity = Context.Set<RoiCtrlEntity>()
                            .Where(x => x.DeviceId == deviceId && x.RoiId == roiId)
                            .FirstOrDefault();
            return Context.Mapper.Map<RoiCtrl>(entity);
        }

        public ICollection<RoiCtrl> FindAll(string deviceId)
        {
            var entities = Context.Set<RoiCtrlEntity>().Where(x => x.DeviceId == deviceId);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<RoiCtrl>>(entities.ToList());
        }

        public new ICollection<RoiCtrl> FindAll(Expression<Func<RoiCtrlEntity, bool>> predicate)
        {
            var entities = Context.Set<RoiCtrlEntity>().Where(predicate);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<RoiCtrl>>(entities.ToList());
        }

        public RoiCtrl Create(RoiCtrl roiCtrl)
        {
            bool exist = Exists(roiCtrl.DeviceId, roiCtrl.RoiId);
            if (exist)
                return null;

            return base.Add(roiCtrl);
        }

        public async Task<RoiCtrl> CreateAsync(RoiCtrl roiCtrl)
        {
            bool exist = Exists(roiCtrl.DeviceId, roiCtrl.RoiId);
            if (exist)
                return null;

            return await base.AddAsync(roiCtrl);
        }

        public RoiCtrl Update(RoiCtrl roiCtrl)
        {
            return base.Update(roiCtrl, roiCtrl.DeviceId, roiCtrl.RoiId);
        }

        public async Task<RoiCtrl> UpdateAsync(RoiCtrl roiCtrl)
        {
            return await base.UpdateAsync(roiCtrl, roiCtrl.DeviceId, roiCtrl.RoiId);
        }

        public int Delete(string deviceId, int roiId)
        {
            return base.Delete(deviceId, roiId);
        }

        public async Task<int> DeleteAsync(string deviceId, int roiId)
        {
            return await base.DeleteAsync(deviceId, roiId);
        }

        #endregion // Public Methods

        #region Disposable

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed == false)
            {
                if (disposing)
                {
                    // Dispose managed objects.
                }

                // Free unmanaged resources and override a finalizer below.
                // Set large fields to null.
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
