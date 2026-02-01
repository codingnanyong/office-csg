using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.EF.Repositories.Production.PCC.Interface;

namespace CSG.MI.FDW.BLL.Production.PCC
{
    public class OpMstRepo : IOpMstRepo
    {
        #region Constructors

        private IOpMstRepository _repo;

        public OpMstRepo(IOpMstRepository repo)
        {
            _repo = repo;
        }

        #endregion

        public ICollection<OpMst> GetAll()
        {
            return _repo.GetAll();
        }

        public async Task<ICollection<OpMst>> GetAllAsync()
        {
            return await _repo.GetAllAsync();
        }

        public OpMst Get(string code, string factory = "DS")
        {
            return _repo.Get(code, factory);
        }

        public async Task<OpMst> GetAsync(string code, string factory = "DS")
        {
            return await _repo.GetAsync(code, factory);
        }

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
