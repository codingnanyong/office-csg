using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL
{
    /// <summary>
    ///
    /// </summary>
    public class BoxRepo : IBoxRepo
    {
        private IBoxRepository _repo;

        public BoxRepo(IBoxRepository repo)
        {
            _repo = repo;
        }

        public Box Get(string ymd, string hms, string deviceId, int boxId)
        {
            Util.ValidateArgs(ymd, hms, deviceId, boxId);

            return _repo.Get(ymd, hms, deviceId, boxId);
        }

        public async Task<Box> GetAsync(string ymd, string hms, string deviceId, int boxId)
        {
            Util.ValidateArgs(ymd, hms, deviceId, boxId);

            return await _repo.GetAsync(ymd, hms, deviceId, boxId);
        }

        public ICollection<Box> FindBy(string ymd, string hms, string deviceId)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return _repo.FindBy(ymd, hms, deviceId);
        }

        public async Task<ICollection<Box>> FindByAsync(string ymd, string hms, string deviceId)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return await _repo.FindByAsync(ymd, hms, deviceId);
        }

        public ICollection<Box> FindBy(DateTime dt, string deviceId)
        {
            Util.ValidateArgs(deviceId);

            return _repo.FindBy(dt, deviceId);
        }

        public async Task<ICollection<Box>> FindByAsync(DateTime dt, string deviceId)
        {
            Util.ValidateArgs(deviceId);

            return await _repo.FindByAsync(dt, deviceId);
        }

        public List<Box> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId = null)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId, boxId);

            return _repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId, boxId).ToList();
        }

        public List<Box> FindBy(DateTime startTime, DateTime endTime, string deviceId, int? boxId = null)
        {
            Util.ValidateArgs(startTime, endTime, deviceId, boxId);

            return _repo.FindBy(startTime, endTime, deviceId, boxId).ToList();
        }

        public async Task<ICollection<Box>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId = null)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId, boxId);

            return await _repo.FindByAsync(startYmd, startHms, endYmd, endHms, deviceId, boxId);
        }

        public async Task<ICollection<Box>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, int? boxId = null)
        {
            Util.ValidateArgs(startTime, endTime, deviceId, boxId);

            return await _repo.FindByAsync(startTime, endTime, deviceId, boxId);
        }

        public int Delete(string ymd, string hms, string deviceId)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return _repo.Delete(ymd, hms, deviceId);
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
