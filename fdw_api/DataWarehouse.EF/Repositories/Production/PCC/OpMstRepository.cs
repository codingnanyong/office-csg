using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using CSG.MI.FDW.EF.Repositories.Production.PCC.Interface;
using Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Repositories.Production.PCC
{
    public class OpMstRepository : GenericMapRepository<MstOpcdEntity, OpMst, HQDbContext>, IOpMstRepository, IDisposable
    {
        #region Constructors
        public OpMstRepository(HQDbContext ctx) : base(ctx)
        {
        }
        #endregion

        #region Public Method

        public OpMst Get(string opcd, string factory = "DS")
        {
            return base.Find(x => x.Factory == factory && x.OpCd == opcd);
        }

        public async Task<OpMst> GetAsync(string opcd, string factory = "DS")
        {
           return await base.FindAsync(x => x.Factory == factory && x.OpCd == opcd);
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
