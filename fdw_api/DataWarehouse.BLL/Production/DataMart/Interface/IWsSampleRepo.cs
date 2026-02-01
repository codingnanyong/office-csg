using CSG.MI.DTO.Production;

namespace CSG.MI.FDW.BLL.Production.DataMart.Interface
{
    public interface IWsSampleRepo : IDisposable
    {
        #region Sample

        ICollection<Sample> GetSamples();

        Task<ICollection<Sample>> GetSamplesAsync();

        Sample GetSample(string wsno, string factory = "DS");

        Task<Sample> GetSampleAsync(string wsno, string factory = "DS");

        ICollection<Sample> SearchSamples(string keyword);

        Task<ICollection<Sample>> SearchSamplesAsync(string keyword);

        ICollection<Sample> GetSamplesByOp(string opcd);

        Task<ICollection<Sample>> GetSamplesByOpAsync(string opcd);

        ICollection<Sample> GetSamplesByOpStatus(string opcd, string status);

        Task<ICollection<Sample>> GetSamplesByOpStatusAsync(string opcd, string status);

        #endregion
    }
}
