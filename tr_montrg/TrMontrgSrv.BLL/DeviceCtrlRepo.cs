using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL
{
    /// <summary>
    ///
    /// </summary>
    public class DeviceCtrlRepo : IDeviceCtrlRepo
    {
        private IDeviceCtrlRepository _repo;

        public DeviceCtrlRepo(IDeviceCtrlRepository repo)
        {
            _repo = repo;
        }

        public bool Exists(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Exists(deviceId);
        }

        public DeviceCtrl Get(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Get(deviceId);
        }

        public List<DeviceCtrl> GetAll()
        {
            return _repo.GetAll().ToList();
        }

        public DeviceCtrl Create(DeviceCtrl deviceCtrl)
        {
            return _repo.Create(deviceCtrl);
        }

        public async Task<DeviceCtrl> CreateAsync(DeviceCtrl deviceCtrl)
        {
            return await _repo.CreateAsync(deviceCtrl);
        }

        public DeviceCtrl Update(DeviceCtrl deviceCtrl)
        {
            return _repo.Update(deviceCtrl);
        }

        public async Task<DeviceCtrl> UpdateAsync(DeviceCtrl deviceCtrl)
        {
            return await _repo.UpdateAsync(deviceCtrl);
        }

        public int Delete(string deviceId)
        {
            return _repo.Delete(deviceId);
        }

        public async Task<int> DeleteAsync(string deviceId)
        {
            return await _repo.DeleteAsync(deviceId);
        }

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
    }
}
