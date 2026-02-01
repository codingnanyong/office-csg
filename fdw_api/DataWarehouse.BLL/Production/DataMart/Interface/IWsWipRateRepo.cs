using CSG.MI.DTO.Production;

namespace CSG.MI.FDW.BLL.Production.DataMart.Interface
{
    public interface IWsWipRateRepo : IDisposable
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
