using CSG.MI.DAO.Production.PCC;
using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface;
using Data;
using Microsoft.EntityFrameworkCore;
using System.Linq.Dynamic.Core;
using System.Numerics;

namespace CSG.MI.FDW.EF.Repositories.Production.RTLS
{
    public class ErpPlanRepository : GenericMapRepository<TbErpPlanDataEntity, ErpPlan, HQDbContext>, IErpPlanRepository, IDisposable
    {
        #region Constructors

        public ErpPlanRepository(HQDbContext ctx) : base(ctx)
        {

        }
        #endregion

        #region Public Methods

        public ICollection<ErpPlan> GetCurrentAll()
        {
            var plans = _context.erpPlns.Where(x => x.BeaconTagId != null) 
                                        .GroupBy(x => x.BeaconTagId)
                                        .Select(g => g.OrderByDescending(x => x.UpdateDate).FirstOrDefault())
                                        .ToList();

            var result = _context.Mapper.Map<ICollection<ErpPlan>>(plans);

            return result;
        }


        public async Task<ICollection<ErpPlan>> GetCurrentAllAsync()
        {
            var plans = await _context.erpPlns.Where(x => x.BeaconTagId != null) 
                                              .GroupBy(x => x.BeaconTagId)
                                              .Select(g => g.OrderByDescending(x => x.UpdateDate).FirstOrDefault())
                                              .ToListAsync();

            var result = _context.Mapper.Map<ICollection<ErpPlan>>(plans);

            return result;
        }

        public ErpPlan GetCurrent(string tagid)
        {
            var plan = _context.erpPlns.Where(x => x.BeaconTagId == tagid)
                                       .OrderByDescending(x => x.UpdateDate)
                                       .FirstOrDefault();

            var result = _context.Mapper.Map<ErpPlan>(plan);

            return result;
        }

        public async Task<ErpPlan> GetCurrentAsync(string tagid)
        {
            var plan = await _context.erpPlns.Where(x => x.BeaconTagId == tagid)
                                             .OrderByDescending(x => x.UpdateDate)
                                             .FirstOrDefaultAsync();

            var result = _context.Mapper.Map<ErpPlan>(plan);

            return result;
        }

        public ICollection<ErpPlan> GetSearch(string? wsno, string? strPblDt, string? endPblDt, string? pm, string? season, string? ctg, string? model, string? stcd, string? clr,
                                              string? size, string? gnd, string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid)
        {
            var query = _context.erpPlns.AsQueryable();

            if (wsno != null)
            {
                query = query.Where("Wsno == @0", wsno);
            }

            if (strPblDt != null && endPblDt != null)
            {
                DateTime start = DateTime.Parse(strPblDt);
                DateTime end = DateTime.Parse(endPblDt).AddDays(1);
                query = query.Where(x => x.UpdateDate >= start && x.UpdateDate < end);
            }
            else if (strPblDt != null)
            {
                DateTime start = DateTime.Parse(strPblDt);
                query = query.Where(x => x.UpdateDate >= start);
            }
            else if (endPblDt != null)
            {
                DateTime end = DateTime.Parse(endPblDt).AddDays(1);
                query = query.Where(x => x.UpdateDate < end);
            }

            if (pm != null)
            {
                query = query.Where("Pm == @0", pm);
            }

            if (season != null)
            {
                query = query.Where("Season == @0", season);
            }

            if (ctg != null)
            {
                query = query.Where("Category == @0", ctg);
            }

            if (model != null)
            {
                query = query.Where("Modelname == @0", model);
            }

            if (stcd != null)
            {
                query = query.Where("Stcd == @0", stcd);
            }

            if (clr != null)
            {
                query = query.Where("ColorwayId == @0", clr);
            }

            if (size != null)
            {
                query = query.Where("SizeCd == @0", size);
            }

            if (gnd != null)
            {
                query = query.Where("Gender == @0", gnd);
            }

            if (prdfc != null)
            {
                query = query.Where("ProdFactory == @0", prdfc);
            }

            if (pst != null)
            {
                query = query.Where("Passtype == @0", pst);
            }

            if (strdueDt != null)
            {
                query = query.Where("SampleEts >= @0", strdueDt);
            }

            if (enddueDt != null)
            {
                query = query.Where("SampleEts <= @0", enddueDt);
            }

            if (strplnDt != null)
            {
                query = query.Where("PlanDate >= @0", strplnDt);
            }

            if (endplnDt != null)
            {
                query = query.Where("PlanDate <= @0", endplnDt);
            }

            if (tagid != null)
            {
                query = query.Where("BeaconTagId == @0", tagid);
            }

            var plans = query.Where(x => x.BeaconTagId != null).ToList();

            var result = _context.Mapper.Map<ICollection<ErpPlan>>(plans);

            return result;
        }


        public async Task<ICollection<ErpPlan>> GetSearchAsync(string? wsno, string? strPblDt, string? endPblDt, string? pm, string? season, string? ctg, string? model, string? stcd, string? clr,
                                                               string? size, string? gnd, string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid)
        {
            var query = _context.erpPlns.AsQueryable();

            if (wsno != null)
            {
                query = query.Where("Wsno == @0", wsno);
            }

            if (strPblDt != null && endPblDt != null)
            {
                DateTime start = DateTime.Parse(strPblDt);
                DateTime end = DateTime.Parse(endPblDt).AddDays(1);
                query = query.Where(x => x.UpdateDate >= start && x.UpdateDate < end);
            }
            else if (strPblDt != null)
            {
                DateTime start = DateTime.Parse(strPblDt);
                query = query.Where(x => x.UpdateDate >= start);
            }
            else if (endPblDt != null)
            {
                DateTime end = DateTime.Parse(endPblDt).AddDays(1);
                query = query.Where(x => x.UpdateDate < end);
            }

            if (pm != null)
            {
                query = query.Where("Pm == @0", pm);
            }

            if (season != null)
            {
                query = query.Where("Season == @0", season);
            }

            if (ctg != null)
            {
                query = query.Where("Category == @0", ctg);
            }

            if (model != null)
            {
                query = query.Where("Modelname == @0", model);
            }

            if (stcd != null)
            {
                query = query.Where("Stcd == @0", stcd);
            }

            if (clr != null)
            {
                query = query.Where("ColorwayId == @0", clr);
            }

            if (size != null)
            {
                query = query.Where("SizeCd == @0", size);
            }

            if (gnd != null)
            {
                query = query.Where("Gender == @0", gnd);
            }

            if (prdfc != null)
            {
                query = query.Where("ProdFactory == @0", prdfc);
            }

            if (pst != null)
            {
                query = query.Where("Passtype == @0", pst);
            }

            if (strdueDt != null)
            {
                query = query.Where("SampleEts >= @0", strdueDt);
            }

            if (enddueDt != null)
            {
                query = query.Where("SampleEts <= @0", enddueDt);
            }

            if (strplnDt != null)
            {
                query = query.Where("PlanDate >= @0", strplnDt);
            }

            if (endplnDt != null)
            {
                query = query.Where("PlanDate <= @0", endplnDt);
            }

            if (tagid != null)
            {
                query = query.Where("BeaconTagId == @0", tagid);
            }

            var plans = await query.Where(x => x.BeaconTagId != null).ToListAsync();

            var result = _context.Mapper.Map<ICollection<ErpPlan>>(plans);

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
