using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.AutoBatch.Interface;
using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.AutoBatch
{
    public class InspectionRepository : GenericMapRepository<DeviceEntity, InspecDevice>, IInspectionRepository, IDisposable
    {
        #region Constructors

        public InspectionRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public ICollection<InspecDevice> GetCheck()
        {
            return GetDevices();
        }

        public async Task<ICollection<InspecDevice>> GetCheckAsync()
        {
            return await Task.Run(() => GetDevices());
        }

        public ICollection<InspecDevice> GetTest()
        {
            return TestDevices();
        }

        #endregion

        #region Private Methods

        private ICollection<InspecDevice> GetDevices()
        {
            var devices = _context.DeviceDbSet.ToList();

            var use_devices = _context.curDeviceSet.ToList();

            var devicesNotInUse = devices.Where(device => !use_devices.Any(useDevice => useDevice.DeviceId == device.DeviceId)).ToList();

            return _context.Mapper.Map<ICollection<InspecDevice>>(devicesNotInUse);
        }

        private ICollection<InspecDevice> TestDevices()
        {
            var devices = _context.DeviceDbSet.Where(x => x.PlantId == "HQ");
            return _context.Mapper.Map<ICollection<InspecDevice>>(devices);
        }

        #endregion
    }
}
