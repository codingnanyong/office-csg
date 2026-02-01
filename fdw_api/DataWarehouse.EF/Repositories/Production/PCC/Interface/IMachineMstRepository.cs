using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.PCC.Interface
{
    public interface IMachineMstRepository : IGenericMapRepository<MstMachineEntity, MachineMst, HQDbContext>
    {
        MachineMst Get(decimal seq);

        Task<MachineMst> GetAsync(decimal seq);
    }
}