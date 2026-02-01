using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart
{
    public class FactoryPrfTotalRepository : GenericMapRepository<FactoryPrfTotalEntity, FactoryTotal, HQDbContext>, IFactoryPrfTotalRepository, IDisposable
    {
        #region Constructors

        public FactoryPrfTotalRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Method

        public ICollection<FactoryTotal> GetPerformaces()
        {
            var tot = _context.PrfTotal.ToList();

            var result = _context.Mapper.Map<ICollection<FactoryTotal>>(tot);

            return result;
        }

        public async Task<ICollection<FactoryTotal>> GetPerformacesAsync()
        {
            var tot = await _context.PrfTotal.ToListAsync();

            var result = _context.Mapper.Map<ICollection<FactoryTotal>>(tot);

            return result;
        }

        public ICollection<FactoryTotal> GetByFactory(string factory = "DS")
        {
            var tot = _context.PrfTotal.Where(x => x.Factory == factory).ToList();

            var result = _context.Mapper.Map<ICollection<FactoryTotal>>(tot);

            return result;
        }

        public async Task<ICollection<FactoryTotal>> GetByFactoryAsync(string factory = "DS")
        {
            var tot = await _context.PrfTotal.Where(x => x.Factory == factory).ToListAsync();

            var result = _context.Mapper.Map<ICollection<FactoryTotal>>(tot);

            return result;
        }

        public FactoryTotal GetByYearFactory(string year, string factory = "DS")
        {
            var tot = _context.PrfTotal.Where(x => x.Factory == factory
                                                && x.Year == year).FirstOrDefault();

            var result = _context.Mapper.Map<FactoryTotal>(tot);

            return result;
        }

        public async Task<FactoryTotal> GetByYearFactoryAsync(string year, string factory = "DS")
        {
            var tot = await _context.PrfTotal.Where(x => x.Factory == factory
                                                      && x.Year == year).FirstOrDefaultAsync();

            var result = _context.Mapper.Map<FactoryTotal>(tot);

            return result;
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
