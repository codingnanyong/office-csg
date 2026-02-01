using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;

namespace CSG.MI.TrMontrgSrv.BLL
{
    /// <summary>
    ///
    /// </summary>
    public class RoiRepo : IRoiRepo
    {
        private IRoiRepository _repo;

        public RoiRepo(IRoiRepository repo)
        {
            _repo = repo;
        }

        public Roi Get(string ymd, string hms, string deviceId, int roiId)
        {
            Util.ValidateArgs(ymd, hms, deviceId, roiId);

            return _repo.Get(ymd, hms, deviceId, roiId);
        }

        public async Task<Roi> GetAsync(string ymd, string hms, string deviceId, int roiId)
        {
            Util.ValidateArgs(ymd, hms, deviceId, roiId);

            return await _repo.GetAsync(ymd, hms, deviceId, roiId);
        }

        public List<ImrData> GetImrChartData(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int roiId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId, roiId);

            return _repo.GetImrChartData(startYmd, startHms, endYmd, endHms, deviceId, roiId);
        }

        public async Task<List<ImrData>> GetImrChartDataAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int roiId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId, roiId);

            return await _repo.GetImrChartDataAsync(startYmd, startHms, endYmd, endHms, deviceId, roiId);
        }

        public ICollection<Roi> FindBy(string ymd, string hms, string deviceId)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return _repo.FindBy(ymd, hms, deviceId);
        }

        public async Task<ICollection<Roi>> FindByAsync(string ymd, string hms, string deviceId)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return await _repo.FindByAsync(ymd, hms, deviceId);
        }

        public ICollection<Roi> FindBy(DateTime dt, string deviceId)
        {
            Util.ValidateArgs(deviceId);

            return _repo.FindBy(dt, deviceId);
        }

        public async Task<ICollection<Roi>> FindByAsync(DateTime dt, string deviceId)
        {
            Util.ValidateArgs(deviceId);

            return await _repo.FindByAsync(dt, deviceId);
        }

        public List<Roi> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? roiId = null)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId, roiId);

            return _repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId, roiId).ToList();
        }

        public List<Roi> FindBy(DateTime startTime, DateTime endTime, string deviceId, int? roiId = null)
        {
            Util.ValidateArgs(startTime, endTime, deviceId, roiId);

            return _repo.FindBy(startTime, endTime, deviceId, roiId).ToList();
        }

        public async Task<ICollection<Roi>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? roiId = null)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId, roiId);

            return await _repo.FindByAsync(startYmd, startHms, endYmd, endHms, deviceId, roiId);
        }

        public async Task<ICollection<Roi>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, int? roiId = null)
        {
            Util.ValidateArgs(startTime, endTime, deviceId, roiId);

            return await _repo.FindByAsync(startTime, endTime, deviceId, roiId);
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
