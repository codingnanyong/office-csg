using CSG.MI.DAO.Production.RTLS;

namespace CSG.MI.FDW.BLL.Production.RTLS.Interface
{
    public interface IErpPlanRepo : IDisposable
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
