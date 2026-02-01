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
    public class MediumRepo : IMediumRepo
    {
        private IMediumRepository _repo;

        public MediumRepo(IMediumRepository repo)
        {
            _repo = repo;
        }

        public Medium Get(string ymd, string hms, string deviceId, MediumType mediumType)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return _repo.Get(ymd, hms, deviceId, mediumType.ToString());
        }

        public async Task<Medium> GetAsync(string ymd, string hms, string deviceId, MediumType mediumType)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return await _repo.GetAsync(ymd, hms, deviceId, mediumType.ToString());
        }

        public List<Medium> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, MediumType? mediumType = null)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return _repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId, mediumType?.ToString()).ToList();
        }

        public List<Medium> FindBy(DateTime startTime, DateTime endTime, string deviceId, MediumType? mediumType = null)
        {
            Util.ValidateArgs(startTime, endTime, deviceId);

            return _repo.FindBy(startTime, endTime, deviceId, mediumType?.ToString()).ToList();
        }

        public async Task<ICollection<Medium>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, MediumType? mediumType = null)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return await _repo.FindByAsync(startYmd, startHms, endYmd, endHms, deviceId, mediumType?.ToString());
        }

        public async Task<ICollection<Medium>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, MediumType? mediumType = null)
        {
            Util.ValidateArgs(startTime, endTime, deviceId);

            return await _repo.FindByAsync(startTime, endTime, deviceId, mediumType?.ToString());
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
