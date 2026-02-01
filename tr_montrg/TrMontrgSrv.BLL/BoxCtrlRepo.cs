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
    public class BoxCtrlRepo : IBoxCtrlRepo
    {
        private IBoxCtrlRepository _repo;

        public BoxCtrlRepo(IBoxCtrlRepository repo)
        {
            _repo = repo;
        }

        public bool Exists(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Exists(deviceId);
        }

        public BoxCtrl Get(string deviceId)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Get(deviceId);
        }

        public List<BoxCtrl> GetAll()
        {
            return _repo.GetAll().ToList();
        }

        public BoxCtrl Create(BoxCtrl boxCtrl)
        {
            return _repo.Create(boxCtrl);
        }

        public async Task<BoxCtrl> CreateAsync(BoxCtrl boxCtrl)
        {
            return await _repo.CreateAsync(boxCtrl);
        }

        public BoxCtrl Update(BoxCtrl boxCtrl)
        {
            return _repo.Update(boxCtrl);
        }

        public async Task<BoxCtrl> UpdateAsync(BoxCtrl boxCtrl)
        {
            return await _repo.UpdateAsync(boxCtrl);
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
