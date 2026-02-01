using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.EF.Repositories.Production.PCC.Interface;

namespace CSG.MI.FDW.BLL.Production.PCC
{
    public class IssueMstRepo : IIssueMstRepo
    {
        #region Constructors

        private IIssueMstRepository _repo;

        public IssueMstRepo(IIssueMstRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Get Methods

        public ICollection<IssueMst> GetAll()
        {
            return _repo.GetAll();
        }

        public async Task<ICollection<IssueMst>> GetAllAsync()
        {
            return await _repo.GetAllAsync();
        }

        public IssueMst Get(string code, string factory = "DS")
        {
           return _repo.Get(code, factory);
        }

        public async Task<IssueMst> GetAsync(string code, string factory = "DS")
        {
            return await _repo.GetAsync(code, factory);
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
