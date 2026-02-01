using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.EF.Repositories.Production.PCC.Interface;

namespace CSG.MI.FDW.BLL.Production.PCC
{
    public class MachineMstRepo : IMachineMstRepo
    {
        #region Contructors

        private IMachineMstRepository _repo;

        public MachineMstRepo(IMachineMstRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Get Methods

        public ICollection<MachineMst> GetAll()
        {
            return _repo.GetAll();
        }

        public async Task<ICollection<MachineMst>> GetAllAsync()
        {
            return  await _repo.GetAllAsync();
        }

        public MachineMst Get(decimal seq)
        {
            return _repo.Get(seq);
        }

        public async Task<MachineMst> GetAsync(decimal seq)
        {
            return await _repo.GetAsync(seq);
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
                    _repo = null!;
                }
            }
        }
        #endregion
    }
}
