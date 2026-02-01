using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using CSG.MI.TrMontrgSrv.EF.Repositories.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Dashboard
{
    public class DashboardRepository : GenericMapRepository<CurDeviceEntity, CurDevice>, IDashboardRepository, IDisposable
    {
        #region Constructors

        public DashboardRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public ICollection<CurDevice> GetDevices()
        {
            return GetCurDevices();
        }

        public async Task<ICollection<CurDevice>> GetDevicesAsync()
        {
            return await Task.Run(() => GetCurDevices());
        }

        public ICollection<CurDevice> GetByFactory(string factory)
        {
            return GetByFactoryDevices(factory);
        }

        public async Task<ICollection<CurDevice>> GetByFactoryAsync(string factory)
        {
            return await Task.Run(() => GetByFactoryDevices(factory));
        }

        public CurDevice GetDeviceById(string factory, string devuce_id)
        {
            return GetCurDevice(factory, devuce_id);
        }

        public async Task<CurDevice> GetDeviceByIdAsync(string factory, string devuce_id)
        {
            return await Task.Run(() => GetCurDevice(factory, devuce_id));
        }

        #endregion

        #region Private Methods

        private ICollection<CurDevice> GetCurDevices()
        {
            var device_entities = _context.curDeviceSet.ToList();

            var devices = _context.Mapper.Map<ICollection<CurDevice>>(device_entities);

            var frames = _context.curFrameSet.ToList();

            foreach (var device in devices)
            {
                foreach (var frame in frames)
                {
                    if (device.DeviceId == frame.DeviceId)
                        device.Frame = _context.Mapper.Map<CurFrame>(frame);
                }
            }

            return devices;
        }

        private ICollection<CurDevice> GetByFactoryDevices(string factory)
        {
            var device_entities = _context.curDeviceSet.Where(x => x.PlantId == factory.ToUpper())
                                                       .ToList();

            var devices = _context.Mapper.Map<ICollection<CurDevice>>(device_entities);

            var frames = _context.curFrameSet.ToList();

            foreach (var device in devices)
            {
                foreach (var frame in frames)
                {
                    if (device.DeviceId == frame.DeviceId)
                        device.Frame = _context.Mapper.Map<CurFrame>(frame);
                }
            }

            return devices;
        }

        private CurDevice GetCurDevice(string factory, string device_id)
        {
            var device_entity = _context.curDeviceSet.Where(x => x.DeviceId == device_id
                                                              && x.PlantId == factory.ToUpper())
                                                     .FirstOrDefault();
            var device = _context.Mapper.Map<CurDevice>(device_entity);

            var frames = _context.curFrameSet.ToList();

            foreach (var frame in frames)
            {
                if (device.DeviceId == frame.DeviceId)
                    device.Frame = _context.Mapper.Map<CurFrame>(frame);
            }

            return device;
        }

        #endregion
    }
}
