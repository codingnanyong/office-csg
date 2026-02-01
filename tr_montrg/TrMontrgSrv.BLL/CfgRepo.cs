using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    public class CfgRepo : ICfgRepo
    {
        private ICfgRepository _repo;

        public CfgRepo(ICfgRepository repo)
        {
            _repo = repo;
        }

        public Cfg Get(int id)
        {
            if (id < 0)
                throw new ArgumentException("ID cannot be less than zero.", innerException: null);

            return _repo.Get(id);
        }

        public async Task<Cfg> GetAsync(int id)
        {
            if (id < 0)
                throw new ArgumentException("ID cannot be less than zero.", innerException: null);

            return await _repo.GetAsync(id);
        }

        public Cfg GetLast(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Device ID cannot be empty or null.", innerException: null);

            return _repo.GetLast(deviceId);
        }

        public List<Cfg> GetLast(string deviceId, int last)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Device ID cannot be empty or null.", innerException: null);

            if (last < 1)
                throw new ArgumentException("Last should be greater than zero.", innerException: null);

            return _repo.GetLast(deviceId, last).ToList();
        }
        public List<Cfg> FindBy(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Device ID cannot be empty or null.", innerException: null);

            return _repo.FindBy(deviceId).ToList();
        }

        public async Task<ICollection<Cfg>> FindByAsync(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Device ID cannot be empty or null.", innerException: null);

            return await _repo.FindByAsync(deviceId);
        }

        //public Dictionary<int, int[]> GetRoi(string deviceId)
        //{
        //    Dictionary<int, int[]> dic = new();

        //    var cfg = GetLast(deviceId);
        //    foreach (var kvp in cfg.CfgJson.RoiCoord)
        //        dic.Add(Convert.ToInt32(kvp.Key), kvp.Value);

        //    return dic;
        //}

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
