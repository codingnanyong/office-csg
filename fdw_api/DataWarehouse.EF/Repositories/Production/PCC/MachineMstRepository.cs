using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using CSG.MI.FDW.EF.Repositories.Production.PCC.Interface;
using CSG.MI.DAO.Production.PCC;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.PCC
{
    public class MachineMstRepository : GenericMapRepository<MstMachineEntity, MachineMst, HQDbContext>, IMachineMstRepository, IDisposable
    {
        #region Constructors

        public MachineMstRepository(HQDbContext ctx) : base(ctx)
        {
        }
        #endregion

        #region Public Methods

        public MachineMst Get(decimal seq)
        {
            return  base.Find(x => x.Seq == seq);
        }

        public async Task<MachineMst> GetAsync(decimal seq)
        {
            return await base.FindAsync(x => x.Seq == seq);
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
