using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart
{
    public class FactoryPlnTotalRepository : GenericMapRepository<FactoryPlnTotalEntity, FactoryTotal, HQDbContext>, IFactoryPlnTotalRepository, IDisposable
    {
        #region Constructors

        public FactoryPlnTotalRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Method

        public ICollection<FactoryTotal> GetPlans()
        {
            return base.GetAll();
        }

        public async Task<ICollection<FactoryTotal>> GetPlansAsync()
        {
            return await base.GetAllAsync();
        }

        public ICollection<FactoryTotal> GetByFactory(string factory = "DS")
        {
            return base.FindBy(x=> x.Factory == factory);
        }

        public async Task<ICollection<FactoryTotal>> GetByFactoryAsync(string factory = "DS")
        {
           return await base.FindByAsync(x=> x.Factory == factory);
        }

        public FactoryTotal GetByYearFactory(string year, string factory = "DS")
        {
            return base.Find(x => x.Factory == factory && x.Year == year);
        }

        public async Task<FactoryTotal> GetByYearFactoryAsync(string year, string factory = "DS")
        {
           return await base.FindAsync(x => x.Factory == factory && x.Year == year);
        }

        #endregion

        #region Disposable

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed == false)
            {
                if (disposing)
                {
                }
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
