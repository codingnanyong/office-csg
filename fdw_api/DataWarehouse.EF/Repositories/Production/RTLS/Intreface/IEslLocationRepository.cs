using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Data;
namespace CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface
{
    public interface IEslLocationRepository : IGenericMapRepository<TbEslLocationEntity, EslLocation, HQDbContext>
    {
        ICollection<EslLocation> GetCurrentAll();

        Task<ICollection<EslLocation>> GetCurrentAllAsync();

        EslLocation GetCurrent(string tagid);

        Task<EslLocation> GetCurrentAsync(string tagid);
    }
}
