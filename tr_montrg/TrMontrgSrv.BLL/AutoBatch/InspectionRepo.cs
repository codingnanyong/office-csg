using CSG.MI.TrMontrgSrv.BLL.AutoBatch.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.AutoBatch.Interface;
using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.BLL.AutoBatch
{
    public class InspectionRepo : IInspectionRepo
    {
        #region Constructors

        private IInspectionRepository _repo;

        public InspectionRepo(IInspectionRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Methods

        public ICollection<InspecDevice> GetCheck()
        {
            return _repo.GetCheck();
        }

        public async Task<ICollection<InspecDevice>> GetCheckAsync()
        {
            return await _repo.GetCheckAsync();
        }

        public ICollection<InspecDevice> GetTest()
        {
            return _repo.GetTest();
        }

        #endregion

        #region Dispose

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

        #endregion
    }
}
