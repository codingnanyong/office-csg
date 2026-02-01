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
    public class DeviceCtrlRepository : GenericMapRepository<DeviceCtrlEntity, DeviceCtrl>, IDeviceCtrlRepository, IDisposable
    {
        #region Constructors

        public DeviceCtrlRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(string deviceId)
        {
            return Context.Set<DeviceCtrlEntity>().Any(x => x.DeviceId == deviceId);
        }

        public DeviceCtrl Get(string deviceId)
        {
            var entity = Context.Set<DeviceCtrlEntity>()
                            .Where(x => x.DeviceId == deviceId)
                            .FirstOrDefault();
            return Context.Mapper.Map<DeviceCtrl>(entity);
        }

        public new ICollection<DeviceCtrl> GetAll()
        {
            var entities = Context.Set<DeviceCtrlEntity>();
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<DeviceCtrl>>(entities.ToList());
        }

        public new ICollection<DeviceCtrl> FindAll(Expression<Func<DeviceCtrlEntity, bool>> predicate)
        {
            var entities = Context.Set<DeviceCtrlEntity>().Where(predicate);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<DeviceCtrl>>(entities.ToList());
        }

        public DeviceCtrl Create(DeviceCtrl deviceCtrl)
        {
            bool exist = Exists(deviceCtrl.DeviceId);
            if (exist)
                return null;

            return base.Add(deviceCtrl);
        }

        public async Task<DeviceCtrl> CreateAsync(DeviceCtrl deviceCtrl)
        {
            bool exist = Exists(deviceCtrl.DeviceId);
            if (exist)
                return null;

            return await base.AddAsync(deviceCtrl);
        }

        public DeviceCtrl Update(DeviceCtrl deviceCtrl)
        {
            return base.Update(deviceCtrl, deviceCtrl.DeviceId);
        }

        public async Task<DeviceCtrl> UpdateAsync(DeviceCtrl deviceCtrl)
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
