using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface
{
    public interface IDailyStatusRepository : IGenericMapRepository<DailyStatusEntity, DailyStatus, HQDbContext>
    {
        #region DailyStatus

        ICollection<DailyStatus> GetDailyStatus();

        Task<ICollection<DailyStatus>> GetDailyStatusAsync();

        ICollection<DailyStatus> GetDailyStatusByFactory(string factory = "DS");

        Task<ICollection<DailyStatus>> GetDailyStatusByFactoryAsync(string factory = "DS");

        DailyStatus GetDailyStatus(string opcd, string factory = "DS");

        Task<DailyStatus> GetDailyStatusAsync(string opcd, string factory = "DS");

        #endregion

        #region DailyStatus/Plan & Prod WorkList

        ICollection<SampleWork> GetWorkList();

        Task<ICollection<SampleWork>> GetWorkListAsync();

        ICollection<SampleWork> GetWorkList(string opcd, string factory = "DS");

        Task<ICollection<SampleWork>> GetWorkListAsync(string opcd, string factory = "DS");

        ICollection<SampleWork> GetSearch(string opcd, string? Keyword, string factory = "DS");

        Task<ICollection<SampleWork>> GetSearchAsync(string opcd, string? Keyword, string factory = "DS");

        #endregion
    }
}