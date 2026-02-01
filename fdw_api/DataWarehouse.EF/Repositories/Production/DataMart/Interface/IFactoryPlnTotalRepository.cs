using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface
{
    public interface IFactoryPlnTotalRepository : IGenericMapRepository<FactoryPlnTotalEntity, FactoryTotal, HQDbContext>
    {
        ICollection<FactoryTotal> GetPlans();

        Task<ICollection<FactoryTotal>> GetPlansAsync();

        ICollection<FactoryTotal> GetByFactory(string factory = "DS");

        Task<ICollection<FactoryTotal>> GetByFactoryAsync(string factory = "DS");

        FactoryTotal GetByYearFactory(string year, string factory = "DS");

        Task<FactoryTotal> GetByYearFactoryAsync(string year, string factory = "DS");
    }
}
