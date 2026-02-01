using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface;
using Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Repositories.Production.RTLS
{
    public class EslLocationRepository : GenericMapRepository<TbEslLocationEntity, EslLocation, HQDbContext>, IEslLocationRepository, IDisposable
    {
        #region Constructors

        public EslLocationRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public ICollection<EslLocation> GetCurrentAll()
        {
            var locations = _context.locations.GroupBy(x => x.BeaconTagId)
                                              .Select(g => g.OrderByDescending(x => x.UpdateDt).FirstOrDefault())
                                              .ToList();

            var result = _context.Mapper.Map<ICollection<EslLocation>>(locations);

            return result;
        }

        public async Task<ICollection<EslLocation>> GetCurrentAllAsync()
        {
            var locations = await _context.locations.GroupBy(x => x.BeaconTagId)
                                                    .Select(g => g.OrderByDescending(x => x.UpdateDt).FirstOrDefault())
                                                    .ToListAsync();

            var result = _context.Mapper.Map<ICollection<EslLocation>>(locations);

            return result;
        }

        public EslLocation GetCurrent(string tagid)
        {
            var location = _context.locations.Where(x => x.BeaconTagId == tagid)
                                             .OrderByDescending(x => x.UpdateDt)
                                             .FirstOrDefault();

            var result = _context.Mapper.Map<EslLocation>(location);

            return result;
        }

        public async Task<EslLocation> GetCurrentAsync(string tagid)
        {
            var location = await _context.locations.Where(x => x.BeaconTagId == tagid)
                                                   .OrderByDescending(x => x.UpdateDt)
                                                   .FirstOrDefaultAsync();

            var result = _context.Mapper.Map<EslLocation>(location);

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
                    // Dispose managed objects.
                }

                // Free unmanaged resources and override a finalizer below.
                // Set large fields to null.
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
