using CSG.MI.DTO.Production;

namespace CSG.MI.FDW.BLL.Production.DataMart.Interface
{
    public interface IDailyStatusRepo : IDisposable
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

        ICollection<SampleWork> GetSearch(string opcd, string? keyword, string factory = "DS");

        Task<ICollection<SampleWork>> GetSearchAsync(string opcd, string? keyword, string factory = "DS");

        #endregion
    }
}
