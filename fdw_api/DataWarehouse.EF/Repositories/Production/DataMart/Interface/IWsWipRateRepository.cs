using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface
{
    public interface IWsWipRateRepository : IGenericMapRepository<WsRateEntity, WipRate, HQDbContext>
    {
        #region WIP Rate

        ICollection<WipRate> GetWipRates();

        Task<ICollection<WipRate>> GetWipRatesAsync();

        WipRate GetWipRate(string opcd);

        Task<WipRate> GetWipRateAsync(string opcd);

        #endregion

        #region WIP Rate : WS List

        ICollection<SampleWork> GetSampleKeys(string opcd, string status);

        Task<ICollection<SampleWork>> GetSampleKeysAsync(string opcd, string status);

        #endregion
    }
}
