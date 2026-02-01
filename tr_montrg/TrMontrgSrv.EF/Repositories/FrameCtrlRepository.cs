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
    public class FrameCtrlRepository : GenericMapRepository<FrameCtrlEntity, FrameCtrl>, IFrameCtrlRepository, IDisposable
    {
        #region Constructors

        public FrameCtrlRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(string deviceId)
        {
            return Context.Set<FrameCtrlEntity>().Any(x => x.DeviceId == deviceId);
        }

        public FrameCtrl Get(string deviceId)
        {
            var entity = Context.Set<FrameCtrlEntity>()
                            .Where(x => x.DeviceId == deviceId)
                            .FirstOrDefault();
            return Context.Mapper.Map<FrameCtrl>(entity);
        }

        public new ICollection<FrameCtrl> GetAll()
        {
            var entities = Context.Set<FrameCtrlEntity>();
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<FrameCtrl>>(entities.ToList());
        }

        public new ICollection<FrameCtrl> FindAll(Expression<Func<FrameCtrlEntity, bool>> predicate)
        {
            var entities = Context.Set<FrameCtrlEntity>().Where(predicate);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<FrameCtrl>>(entities.ToList());
        }

        public FrameCtrl Create(FrameCtrl deviceCtrl)
        {
            bool exist = Exists(deviceCtrl.DeviceId);
            if (exist)
                return null;

            return base.Add(deviceCtrl);
        }

        public async Task<FrameCtrl> CreateAsync(FrameCtrl deviceCtrl)
        {
            bool exist = Exists(deviceCtrl.DeviceId);
            if (exist)
                return null;

            return await base.AddAsync(deviceCtrl);
        }

        public FrameCtrl Update(FrameCtrl deviceCtrl)
        {
            return base.Update(deviceCtrl, deviceCtrl.DeviceId);
        }

        public async Task<FrameCtrl> UpdateAsync(FrameCtrl deviceCtrl)
        {
            return await base.UpdateAsync(deviceCtrl, deviceCtrl.DeviceId);
        }

        public int Delete(string deviceId)
        {
            return base.Delete(deviceId);
        }

        public async Task<int> DeleteAsync(string deviceId)
        {
            return await base.DeleteAsync(deviceId);
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
