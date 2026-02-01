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
    public class GrpKeyRepo : IGrpKeyRepo
    {
        private IGrpKeyRepository _repo;

        public GrpKeyRepo(IGrpKeyRepository repo)
        {
            _repo = repo;
        }

        public bool Exists(string group, string key)
        {
            if (String.IsNullOrWhiteSpace(group) || String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Exists(group, key);
        }

        public GrpKey Get(string group, string key)
        {
            if (String.IsNullOrWhiteSpace(group) || String.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Get(group, key);
        }

        public List<GrpKey> GetAll()
        {
            return _repo.GetAll().ToList();
        }

        public List<GrpKey> FindAll(string group)
        {
            if (String.IsNullOrWhiteSpace(group))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.FindAll(group).ToList();
        }

        public GrpKey Create(GrpKey grpKey)
        {
            return _repo.Create(grpKey);
        }

        public async Task<GrpKey> CreateAsync(GrpKey grpKey)
        {
            return await _repo.CreateAsync(grpKey);
        }

        public GrpKey Update(GrpKey grpKey)
        {
            return _repo.Update(grpKey);
        }

        public async Task<GrpKey> UpdateAsync(GrpKey grpKey)
        {
            return await _repo.UpdateAsync(grpKey);
        }

        public int Delete(string group, string key)
        {
            return _repo.Delete(group, key);
        }

        public async Task<int> DeleteAsync(string group, string key)
        {
            return await _repo.DeleteAsync(group, key);
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
