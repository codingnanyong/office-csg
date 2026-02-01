using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;

namespace CSG.MI.FDW.BLL.Production.DataMart
{
    public class FactoryPrfTotalRepo : IFactoryPrfTotalRepo
    {
        #region Constructors

        private IFactoryPrfTotalRepository _repo;

        public FactoryPrfTotalRepo(IFactoryPrfTotalRepository repo)
        {
            _repo = repo;
        }
        #endregion

        #region Public Method

        public ICollection<FactoryTotal> GetPerformaces()
        {
            return _repo.GetPerformaces();
        }

        public async Task<ICollection<FactoryTotal>> GetPerformacesAsync()
        {
            return await _repo.GetPerformacesAsync();
        }

        public ICollection<FactoryTotal> GetByFactory(string factory = "DS")
        {
            return _repo.GetByFactory(factory);
        }

        public async Task<ICollection<FactoryTotal>> GetByFactoryAsync(string factory = "DS")
        {
            return await _repo.GetByFactoryAsync(factory);
        }

        public FactoryTotal GetByYearFactory(string year, string factory = "DS")
        {
            return _repo.GetByYearFactory(year, factory);
        }

        public async Task<FactoryTotal> GetByYearFactoryAsync(string year, string factory = "DS")
        {
            return await _repo.GetByYearFactoryAsync(year, factory);
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
