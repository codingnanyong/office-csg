using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class DeviceRepository : GenericMapRepository<DeviceEntity, Device>, IDeviceRepository, IDisposable
    {
        #region Constructors

        public DeviceRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(string deviceId)
        {
            return Context.Set<DeviceEntity>().Any(x => x.DeviceId == deviceId);
        }

        public Device Get(string deviceId)
        {
            var entity = Context.Set<DeviceEntity>()
                            .Where(x => x.DeviceId == deviceId)
                            .FirstOrDefault();
            return Context.Mapper.Map<Device>(entity);
        }

        public new ICollection<Device> GetAll()
        {
            var entities = Context.Set<DeviceEntity>();
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<Device>>(entities.ToList());
        }

        public ICollection<Device> FindBy(string plantId, string locationId = null)
        {
            if (locationId == null)
                return base.FindBy(x => x.PlantId == plantId).ToList();

            return base.FindBy(x => x.PlantId == plantId &&
                                    x.LocationId == locationId).ToList();
        }

        public async Task<ICollection<Device>> FindByAsync(string plantId, string locationId = null)
        {
            if (locationId == null)
                return await base.FindByAsync(x => x.PlantId == plantId);

            return await base.FindByAsync(x => x.PlantId == plantId &&
                                               x.LocationId == locationId);
        }

        public new ICollection<Device> FindAll(Expression<Func<DeviceEntity, bool>> predicate)
        {
            var entities = Context.Set<DeviceEntity>().Where(predicate);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<Device>>(entities.ToList());
        }

        public bool CreateAlways(Device device)
        {
            using var transaction = Context.Database.BeginTransaction();
            try
            {
                bool exist = Exists(device.DeviceId);
                if (exist == false)
                {
                    base.Add(device);
                    transaction.Commit();
                    return true;
                }

                using FrameRepository frameRepo = new(Context);
                using RoiRepository roiRepo = new(Context);
                using BoxRepository boxRepo = new(Context);
                using CfgRepository cfgRepo = new(Context);
                using MediumRepository mediumRepo = new(Context);
                {
                    frameRepo.AddRange(device.Frames);
                    roiRepo.AddRange(device.Rois);
                    boxRepo.AddRange(device.Boxes);

                    // Compare with previous configuration
                    var prvCfg = cfgRepo.GetLast(device.DeviceId);
                    var curCfg = device.Cfgs.FirstOrDefault();
                    if (curCfg.CfgJson.Equals(prvCfg?.CfgJson) == false)
                        cfgRepo.AddRange(device.Cfgs);

                    mediumRepo.AddRange(device.Media);

                    transaction.Commit();
                }
            }
            catch
            {
                return false;
            }

            return true;
        }

        public Device Create(Device device)
        {
            bool exist = Exists(device.DeviceId);
            if (exist)
                return null;

            return base.Add(device);
        }

        public async Task<Device> CreateAsync(Device device)
        {
            bool exist = Exists(device.DeviceId);
            if (exist)
                return null;

            return await base.AddAsync(device);
        }

        public Device Update(Device device)
        {
            return base.Update(device, device.DeviceId);
        }

        public async Task<Device> UpdateAsync(Device device)
        {
            return await base.UpdateAsync(device, device.DeviceId);
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
