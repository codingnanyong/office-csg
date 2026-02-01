using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL
{
    /// <summary>
    ///
    /// </summary>
    public class EvntLogRepo : IEvntLogRepo
    {
        private IEvntLogRepository _repo;

        public EvntLogRepo(IEvntLogRepository repo)
        {
            _repo = repo;
        }

        public bool Exists(int id)
        {
            if (id < 0)
                throw new ArgumentOutOfRangeException("ID cannot be less than zero.", innerException: null);

            return _repo.Exists(id);
        }

        public bool Exists(string deviceId, string ymd, string hms)
        {
            if (String.IsNullOrEmpty(deviceId))
                throw new ArgumentNullException("Device ID cannot be null or empty.", innerException: null);

            return _repo.Exists(deviceId, ymd, hms);
        }

        public EvntLog Get(int id)
        {
            if (id < 0)
                throw new ArgumentOutOfRangeException("ID cannot be less than zero.", innerException: null);

            return _repo.Get(id);
        }

        public async Task<EvntLog> GetAsync(int id)
        {
            if (id < 0)
                throw new ArgumentOutOfRangeException("ID cannot be less than zero.", innerException: null);

            return await _repo.GetAsync(id);
        }

        public List<EvntLog> GetLastest(string plantId, string deviceId = null, bool excludingInfoLevel = false)
        {
            if (String.IsNullOrEmpty(plantId))
                throw new ArgumentNullException("Plant ID cannot be null or empty.", innerException: null);

            return _repo.GetLastest(plantId, deviceId, excludingInfoLevel);
        }


        public List<EvntLog> FindBy(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                throw new ArgumentNullException("Device ID cannot be null or empty.", innerException: null);

            return _repo.FindBy(deviceId).ToList();
        }

        public List<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return _repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId).ToList();
        }

        public List<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string evntLvl)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);
            if (String.IsNullOrEmpty(evntLvl))
                throw new ArgumentNullException("Event Level cannot be null or empty.", innerException: null);

            return _repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId, evntLvl).ToList();
        }

        public List<EvntLog> FindBy(DateTime startTime, DateTime endTime, string deviceId)
        {
            Util.ValidateArgs(startTime, endTime, deviceId);

            return _repo.FindBy(startTime, endTime, deviceId).ToList();
        }

        public async Task<ICollection<EvntLog>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, deviceId);

            return await _repo.FindByAsync(startYmd, startHms, endYmd, endHms, deviceId);
        }

        public async Task<ICollection<EvntLog>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId)
        {
            Util.ValidateArgs(startTime, endTime, deviceId);

            return await _repo.FindByAsync(startTime, endTime, deviceId);
        }

        public List<EvntLog> FindByPlant(string plantId)
        {
            if (String.IsNullOrEmpty(plantId))
                throw new ArgumentNullException("Plant ID cannot be null or empty.", innerException: null);

            return _repo.FindByPlant(plantId).ToList();
        }

        public List<EvntLog> FindByPlant(string startYmd, string startHms, string endYmd, string endHms, string plantId)
        {
            Util.ValidateArgs(startYmd, startHms, endYmd, endHms, plantId);

            return _repo.FindByPlant(startYmd, startHms, endYmd, endHms, plantId).ToList();
        }

        public EvntLog Create(EvntLog evntLog)
        {
            return _repo.Create(evntLog);
        }

        public async Task<EvntLog> CreateAsync(EvntLog evntLog)
        {
            return await _repo.CreateAsync(evntLog);
        }

        public EvntLog Update(EvntLog evntLog)
        {
            return _repo.Update(evntLog);
        }

        public async Task<EvntLog> UpdateAsync(EvntLog evntLog)
        {
            return await _repo.UpdateAsync(evntLog);
        }

        public int Delete(int id)
        {
            return _repo.Delete(id);
        }

        public async Task<int> DeleteAsync(int id)
        {
            return await _repo.DeleteAsync(id);
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
