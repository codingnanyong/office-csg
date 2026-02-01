using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface
{
    public interface IErpPlanRepository : IGenericMapRepository<TbErpPlanDataEntity, ErpPlan, HQDbContext>
    {
        ICollection<ErpPlan> GetCurrentAll();

        Task<ICollection<ErpPlan>> GetCurrentAllAsync();

        ErpPlan GetCurrent(string tagid);

        Task<ErpPlan> GetCurrentAsync(string tagid);

        ICollection<ErpPlan> GetSearch(string? wsno, string? strPblDt, string? endPblDt, string? pm, string? season, string? ctg, string? model, string? stcd, string? clr,
                                       string? size, string? gnd, string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid);

        Task<ICollection<ErpPlan>> GetSearchAsync(string? wsno, string? strPblDt, string? endPblDt, string? pm, string? season, string? ctg, string? model, string? stcd, string? clr,
                                                  string? size, string? gnd, string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid);
    }
}