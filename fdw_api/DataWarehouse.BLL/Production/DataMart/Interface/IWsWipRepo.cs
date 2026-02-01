using CSG.MI.DTO.Production;

namespace CSG.MI.FDW.BLL.Production.DataMart.Interface
{
    public interface IWsWipRepo : IDisposable
    {
        #region WIP

        ICollection<Wip> GetAll();

        Task<ICollection<Wip>> GetAllAsync();

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
