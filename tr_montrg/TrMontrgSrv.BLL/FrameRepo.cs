using System;
using System.Collections.Generic;
using System.Linq;
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
    public class FrameRepo : IFrameRepo
    {
        private IFrameRepository _repo;

        public FrameRepo(IFrameRepository repo)
        {
            _repo = repo;
        }

        public Frame Get(string ymd, string hms, string deviceId)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return _repo.Get(ymd, hms, deviceId);
        }

        public async Task<Frame> GetAsync(string ymd, string hms, string deviceId)
        {
            Util.ValidateArgs(ymd, hms, deviceId);

            return await _repo.GetAsync(ymd, hms, deviceId);
        }

        public List<ImrData> GetImrChartData(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return _repo.GetImrChartData(startYmd, startHms, endYmd, endHms, deviceId);
        }

        public async Task<List<ImrData>> GetImrChartDataAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return await _repo.GetImrChartDataAsync(startYmd, startHms, endYmd, endHms, deviceId);
        }

        public List<Frame> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return _repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId).ToList();
        }

        public List<Frame> FindBy(DateTime startTime, DateTime endTime, string deviceId)
        {
            Util.ValidateArgs(startTime, endTime, deviceId);

            return _repo.FindBy(startTime, endTime, deviceId).ToList();
        }

        public async Task<ICollection<Frame>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return await _repo.FindByAsync(startYmd, startHms, endYmd, endHms, deviceId);
        }

        public async Task<ICollection<Frame>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId)
        {
            Util.ValidateArgs(startTime, endTime, deviceId);

            return await _repo.FindByAsync(startTime, endTime, deviceId);
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
