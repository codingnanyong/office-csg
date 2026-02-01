using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Microsoft.EntityFrameworkCore;
using CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface;
using CSG.MI.DAO.Production.RTLS;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.RTLS
{
    public class ProcessMstRepository : GenericMapRepository<TbProcessLocEntity, ProcessMst, HQDbContext>, IProcessMstRepository, IDisposable
    {
        #region Constructors

        public ProcessMstRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public ProcessMst Get(string loc)
        {
            return base.Find(x => x.ProcessLoc == loc);
        }


        public async Task<ProcessMst> GetAsync(string loc)
        {
            return await base.FindAsync(x => x.ProcessLoc == loc);
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
