using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.BLL.Production.RTLS.Interface;
using CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface;

namespace CSG.MI.FDW.BLL.Production.RTLS
{
    public class ErpPlanRepo : IErpPlanRepo
    {
        #region Constructors

        private IErpPlanRepository _repo;

        public ErpPlanRepo(IErpPlanRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Method

        public ICollection<ErpPlan> GetCurrentAll()
        {
            return _repo.GetCurrentAll();
        }

        public async Task<ICollection<ErpPlan>> GetCurrentAllAsync()
        {
            return await _repo.GetCurrentAllAsync();
        }

        public ErpPlan GetCurrent(string tagid)
        {
            return _repo.GetCurrent(tagid);
        }

        public async Task<ErpPlan> GetCurrentAsync(string tagid)
        {
            return await _repo.GetCurrentAsync(tagid);
        }

        public ICollection<ErpPlan> GetSearch(string? wsno, string? strPblDt, string? endPblDt, string? pm, string? season, string? ctg, string? model, string? stcd, string? clr,
                                              string? size, string? gnd, string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid)
        {
            if (string.IsNullOrEmpty(wsno) || string.IsNullOrEmpty(strPblDt) || string.IsNullOrEmpty(endPblDt) || string.IsNullOrEmpty(pm) || string.IsNullOrEmpty(season) ||
                string.IsNullOrEmpty(ctg) || string.IsNullOrEmpty(model) || string.IsNullOrEmpty(stcd) || string.IsNullOrEmpty(clr) ||
                string.IsNullOrEmpty(size) || string.IsNullOrEmpty(gnd) || string.IsNullOrEmpty(prdfc) || string.IsNullOrEmpty(pst) ||
                string.IsNullOrEmpty(tagid) || string.IsNullOrEmpty(strdueDt) || string.IsNullOrEmpty(enddueDt) || string.IsNullOrEmpty(strplnDt) || string.IsNullOrEmpty(endplnDt))
            {
                throw new ArgumentNullException("Argument cannot be empty or null.", innerException: null);
            }

            return _repo.GetSearch(wsno, strPblDt, endPblDt, pm, season, ctg, model, stcd, clr, size, gnd, strdueDt, enddueDt, prdfc, strplnDt, endplnDt, pst, tagid);
        }


        public async Task<ICollection<ErpPlan>> GetSearchAsync(string? wsno, string? strPblDt, string? endPblDt, string? pm, string? season, string? ctg, string? model, string? stcd, string? clr,
                                                               string? size, string? gnd, string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid)
        {

            if (string.IsNullOrEmpty(wsno) || string.IsNullOrEmpty(strPblDt) || string.IsNullOrEmpty(endPblDt) || string.IsNullOrEmpty(pm) || string.IsNullOrEmpty(season) ||
                string.IsNullOrEmpty(ctg) || string.IsNullOrEmpty(model) || string.IsNullOrEmpty(stcd) || string.IsNullOrEmpty(clr) ||
                string.IsNullOrEmpty(size) || string.IsNullOrEmpty(gnd) || string.IsNullOrEmpty(prdfc) || string.IsNullOrEmpty(pst) ||
                string.IsNullOrEmpty(tagid) || string.IsNullOrEmpty(strdueDt) || string.IsNullOrEmpty(enddueDt) || string.IsNullOrEmpty(strplnDt) || string.IsNullOrEmpty(endplnDt))
            {
                throw new ArgumentNullException("Argument cannot be empty or null.", innerException: null);
            }

            return await _repo.GetSearchAsync(wsno, strPblDt, endPblDt, pm, season, ctg, model, stcd, clr, size, gnd, strdueDt, enddueDt, prdfc, strplnDt, endplnDt, pst, tagid);
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
