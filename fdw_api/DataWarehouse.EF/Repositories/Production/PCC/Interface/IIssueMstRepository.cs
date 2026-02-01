using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.PCC.Interface
{
    public interface IIssueMstRepository : IGenericMapRepository<MstPlanIssuecdEntity, IssueMst, HQDbContext>
    {
        IssueMst Get(string issue, string factory = "DS");

        Task<IssueMst> GetAsync(string issue, string factory = "DS");
    }
}
