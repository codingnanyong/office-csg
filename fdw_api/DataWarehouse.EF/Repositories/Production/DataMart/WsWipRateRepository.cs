using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart
{
    public class WsWipRateRepository : GenericMapRepository<WsRateEntity, WipRate, HQDbContext>, IWsWipRateRepository, IDisposable
    {
        #region Constructors

        public WsWipRateRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods - WIP Rates

        public ICollection<WipRate> GetWipRates()
        {
            return base.GetAll();
        }

        public async Task<ICollection<WipRate>> GetWipRatesAsync()
        {
            return await base.GetAllAsync();
        }

        public WipRate GetWipRate(string opcd)
        {
            var entity = _context.wsRate.Where(x => x.OpCd == opcd).FirstOrDefault();

            var result = _context.Mapper.Map<WipRate>(entity);

            return result;
        }

        public async Task<WipRate> GetWipRateAsync(string opcd)
        {
            var entity = await _context.wsRate.Where(x => x.OpCd == opcd).FirstOrDefaultAsync();

            var result = _context.Mapper.Map<WipRate>(entity);

            return result;
        }

        #endregion

        #region Public Methods - WIP Rates : WorkList
        
        public ICollection<SampleWork> GetSampleKeys(string opcd,string status)
        {
            var summaries = _context.wsSummaries.Where(smry => _context.wsStatus
                                                .Any(x => x.OpCd == opcd && x.Status == status && x.Ws == smry.WsNo) && smry.OpCd == opcd)
                                                .ToList();

            var result = _context.Mapper.Map<ICollection<SampleWork>>(summaries);

            return result;

        }

        public async Task<ICollection<SampleWork>> GetSampleKeysAsync(string opcd, string status)
        {
            var summaries = await _context.wsSummaries.Where(smry => _context.wsStatus
                                                      .Any(x => x.OpCd == opcd && x.Status == status && x.Ws == smry.WsNo) && smry.OpCd == opcd)
                                                      .ToListAsync();

            var result = _context.Mapper.Map<ICollection<SampleWork>>(summaries);

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
