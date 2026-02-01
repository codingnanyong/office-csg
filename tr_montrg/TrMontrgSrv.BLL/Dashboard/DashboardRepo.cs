using CSG.MI.TrMontrgSrv.BLL.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.BLL.Dashboard
{
    public class DashboardRepo : IDashboardRepo
    {
        #region Constructors

        private IDashboardRepository _repo;

        public DashboardRepo(IDashboardRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Methods

        public ICollection<CurDevice> FindDevices()
        {
            return _repo.GetDevices();
        }

        public async Task<ICollection<CurDevice>> FindDevicesAsync()
        {
            return await _repo.GetDevicesAsync();
        }

        public ICollection<CurDevice> FindDeviceByFactory(string factory)
        {
            return _repo.GetByFactory(factory);
        }

        public async Task<ICollection<CurDevice>> FindDeviceByFactoryAsync(string factory)
        {
            return await _repo.GetByFactoryAsync(factory);
        }

        public CurDevice FindDeviceById(string factory, string devuce_id)
        {
            return _repo.GetDeviceById(factory, devuce_id);
        }

        public async Task<CurDevice> FindDeviceByIdAsync(string factory, string devuce_id)
        {
            return await _repo.GetDeviceByIdAsync(factory, devuce_id);
        }

        #endregion

        #region Dispose

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_repo != null)
                {
                    _repo.Dispose();
                    _repo = null;
                }
            }
        }

        #endregion
    }
}
