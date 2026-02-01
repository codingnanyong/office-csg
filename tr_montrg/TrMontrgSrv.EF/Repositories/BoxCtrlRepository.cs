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
    public class BoxCtrlRepository : GenericMapRepository<BoxCtrlEntity, BoxCtrl>, IBoxCtrlRepository, IDisposable
    {
        #region Constructors

        public BoxCtrlRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(string deviceId)
        {
            return Context.Set<BoxCtrlEntity>().Any(x => x.DeviceId == deviceId);
        }

        public BoxCtrl Get(string deviceId)
        {
            var entity = Context.Set<BoxCtrlEntity>()
                            .Where(x => x.DeviceId == deviceId)
                            .FirstOrDefault();
            return Context.Mapper.Map<BoxCtrl>(entity);
        }

        public new ICollection<BoxCtrl> GetAll()
        {
            var entities = Context.Set<BoxCtrlEntity>();
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<BoxCtrl>>(entities.ToList());
        }

        public new ICollection<BoxCtrl> FindAll(Expression<Func<BoxCtrlEntity, bool>> predicate)
        {
            var entities = Context.Set<BoxCtrlEntity>().Where(predicate);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<BoxCtrl>>(entities.ToList());
        }

        public BoxCtrl Create(BoxCtrl deviceCtrl)
        {
            bool exist = Exists(deviceCtrl.DeviceId);
            if (exist)
                return null;

            return base.Add(deviceCtrl);
        }

        public async Task<BoxCtrl> CreateAsync(BoxCtrl deviceCtrl)
        {
            bool exist = Exists(deviceCtrl.DeviceId);
            if (exist)
                return null;

            return await base.AddAsync(deviceCtrl);
        }

        public BoxCtrl Update(BoxCtrl deviceCtrl)
        {
            return base.Update(deviceCtrl, deviceCtrl.DeviceId);
        }

        public async Task<BoxCtrl> UpdateAsync(BoxCtrl deviceCtrl)
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
