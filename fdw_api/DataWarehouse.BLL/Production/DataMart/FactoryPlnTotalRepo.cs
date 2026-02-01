using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;

namespace CSG.MI.FDW.BLL.Production.DataMart
{
    public class FactoryPlnTotalRepo : IFactoryPlnTotalRepo
    {
        #region Constructors

        private IFactoryPlnTotalRepository _repo;

        public FactoryPlnTotalRepo(IFactoryPlnTotalRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Method

        public ICollection<FactoryTotal> GetPlans()
        {
            return _repo.GetPlans();
        }

        public async Task<ICollection<FactoryTotal>> GetPlansAsync()
        {
            return await _repo.GetPlansAsync();
        }

        public ICollection<FactoryTotal> GetByFactory(string factory = "DS")
        {
            return _repo.GetByFactory(factory);
        }

        public Task<ICollection<FactoryTotal>> GetByFactoryAsync(string factory = "DS")
        {
            return _repo.GetByFactoryAsync(factory);
        }

        public FactoryTotal GetByYearFactory(string year, string factory = "DS")
        {
            return _repo.GetByYearFactory(year, factory);
        }

        public Task<FactoryTotal> GetByYearFactoryAsync(string year, string factory = "DS")
        {
            return _repo.GetByYearFactoryAsync(year, factory);
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
