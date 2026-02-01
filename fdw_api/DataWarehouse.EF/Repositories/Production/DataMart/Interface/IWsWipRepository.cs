using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface
{
    public interface IWsWipRepository : IGenericMapRepository<WsSummaryEntity, SampleWork, HQDbContext>
    {
        #region WIP

        ICollection<Wip> GetWips();

        Task<ICollection<Wip>> GetWipsAsync();

        ICollection<Wip> GetByFactory(string factory = "DS");

        Task<ICollection<Wip>> GetByFactoryAsync(string factory = "DS");

        Wip GetWip(string opcd, string factory = "DS");

        Task<Wip> GetWipAsync(string opcd, string factory = "DS");

        #endregion

        #region WIP/Prod WorkList

        ICollection<SampleWork> GetWorkList();

        Task<ICollection<SampleWork>> GetWorkListAsync();

        ICollection<SampleWork> GetWorkList(string opcd, string factory = "DS");

        Task<ICollection<SampleWork>> GetWorkListAsync(string opcd, string factory = "DS");

        ICollection<SampleWork> GetSearch(string opcd, string? keyword, string factory = "DS");

        Task<ICollection<SampleWork>> GetSearchAsync(string opcd, string? keyword, string factory = "DS");

        #endregion
    }
}
