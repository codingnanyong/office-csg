using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface
{
    public interface IProcessMstRepository : IGenericMapRepository<TbProcessLocEntity, ProcessMst, HQDbContext>
    {
        ProcessMst Get(string loc);

        Task<ProcessMst> GetAsync(string loc);
    }
}
