using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.Interface;

namespace CSG.MI.TrMontrgSrv.BLL
{
    /// <summary>
    ///
    /// </summary>
    public class RoiCtrlRepo : IRoiCtrlRepo, IRoiImrCtrlRepo
    {
        private IRoiCtrlRepository _repo;

        public RoiCtrlRepo(IRoiCtrlRepository repo)
        {
            _repo = repo;
        }

        public bool Exists(string deviceId, int roiId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Divice ID cannot be empty or null.", innerException: null);

            if (roiId < 0)
                throw new ArgumentOutOfRangeException("ROI ID cannot be less than zero.", innerException: null);

            return _repo.Exists(deviceId, roiId);
        }

        public RoiCtrl Get(string deviceId, int roiId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Divice ID cannot be empty or null.", innerException: null);

            if (roiId < 0)
                throw new ArgumentOutOfRangeException("ROI ID cannot be less than zero.", innerException: null);

            return _repo.Get(deviceId, roiId);
        }

        public IImrCtrl Get(string deviceId, int? id)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Divice ID cannot be empty or null.", innerException: null);

            if (id != null && id < 0)
                throw new ArgumentOutOfRangeException("ROI ID cannot be null or less than zero.", innerException: null);

            return _repo.Get(deviceId, id.Value);
        }

        public List<RoiCtrl> FindAll(string deviceId)
        {
            return _repo.FindAll(deviceId).ToList();
        }

        public RoiCtrl Create(RoiCtrl roiCtrl)
        {
            return _repo.Create(roiCtrl);
        }

        public async Task<RoiCtrl> CreateAsync(RoiCtrl roiCtrl)
        {
            return await _repo.CreateAsync(roiCtrl);
        }

        public RoiCtrl Update(RoiCtrl roiCtrl)
        {
            return _repo.Update(roiCtrl);
        }

        public async Task<RoiCtrl> UpdateAsync(RoiCtrl roiCtrl)
        {
            return await _repo.UpdateAsync(roiCtrl);
        }

        public int Delete(string deviceId, int roiId)
        {
            return _repo.Delete(deviceId, roiId);
        }

        public async Task<int> DeleteAsync(string deviceId, int roiId)
        {
            return await _repo.DeleteAsync(deviceId, roiId);
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
