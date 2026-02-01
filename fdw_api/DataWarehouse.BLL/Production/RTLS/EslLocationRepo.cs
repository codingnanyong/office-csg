using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.BLL.Production.RTLS.Interface;
using CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface;

namespace CSG.MI.FDW.BLL.Production.RTLS
{
    public class EslLocationRepo : IEslLocationRepo
    {
        #region Constructors

        private IEslLocationRepository _repo;

        public EslLocationRepo(IEslLocationRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Method

        public ICollection<EslLocation> GetCurrentAll()
        {
            return _repo.GetCurrentAll();
        }

        public async Task<ICollection<EslLocation>> GetCurrentAllAsync()
        {
            return await _repo.GetCurrentAllAsync();
        }

        public EslLocation GetCurrent(string tagid)
        {
            return _repo.GetCurrent(tagid);
        }

        public async Task<EslLocation> GetCurrentAsync(string tagid)
        {
            return await _repo.GetCurrentAsync(tagid);
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
