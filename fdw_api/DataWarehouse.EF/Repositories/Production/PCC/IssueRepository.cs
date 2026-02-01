using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using CSG.MI.FDW.EF.Repositories.Production.PCC.Interface;
using CSG.MI.DAO.Production.PCC;

namespace CSG.MI.FDW.EF.Repositories.Production.PCC
{
    public class IssueRepository : GenericMapRepository<MstPlanIssuecdEntity, IssueMst>, IIssueMstRepository, IDisposable
    {
        #region Constructors

        public IssueRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public IssueMst Get(string issue, string factory = "DS")
        {
            return base.Find(x => x.Factory == factory && x.IssueCd == issue);
        }

        public async Task<IssueMst> GetAsync(string issue, string factory = "DS")
        {
            return await base.FindAsync(x => x.Factory == factory && x.IssueCd == issue);
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
