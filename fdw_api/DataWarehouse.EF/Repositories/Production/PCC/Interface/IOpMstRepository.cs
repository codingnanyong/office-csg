using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.PCC.Interface
{
    public interface IOpMstRepository : IGenericMapRepository<MstOpcdEntity, OpMst, HQDbContext>
    {
        OpMst Get(string opcd, string factory = "DS");

        Task<OpMst> GetAsync(string opcd, string factory = "DS");
    }
}
