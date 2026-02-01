using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.BLL.Production.RTLS.Interface;
using CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface;

namespace CSG.MI.FDW.BLL.Production.RTLS
{
    public class ProcessMstRepo : IProcessMstRepo
    {
        #region Constructors

        private IProcessMstRepository _repo;


        public ProcessMstRepo(IProcessMstRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Method

        public ICollection<ProcessMst> GetAll()
        {
            return _repo.GetAll();
        }

        public async Task<ICollection<ProcessMst>> GetAllAsync()
        {
            return await _repo.GetAllAsync();
        }

        public ProcessMst Get(string processloc)
        {
            return _repo.Get(processloc);
        }

        public async Task<ProcessMst> GetAsync(string processloc)
        {
            return await _repo.GetAsync(processloc);
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
