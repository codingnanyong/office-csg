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
    public class FrameCtrlRepo : IFrameCtrlRepo, IFrameImrCtrlRepo
    {
        private IFrameCtrlRepository _repo;

        public FrameCtrlRepo(IFrameCtrlRepository repo)
        {
            _repo = repo;
        }

        public bool Exists(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Exists(deviceId);
        }

        public FrameCtrl Get(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Get(deviceId);
        }

        public IImrCtrl Get(string deviceId, int? id)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Get(deviceId);
        }

        public List<FrameCtrl> GetAll()
        {
            return _repo.GetAll().ToList();
        }

        public FrameCtrl Create(FrameCtrl deviceCtrl)
        {
            return _repo.Create(deviceCtrl);
        }

        public async Task<FrameCtrl> CreateAsync(FrameCtrl deviceCtrl)
        {
            return await _repo.CreateAsync(deviceCtrl);
        }

        public FrameCtrl Update(FrameCtrl deviceCtrl)
        {
            return _repo.Update(deviceCtrl);
        }

        public async Task<FrameCtrl> UpdateAsync(FrameCtrl deviceCtrl)
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
