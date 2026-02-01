using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface
{
    public interface IWsSampleRepository : IGenericMapRepository<WsEntity, Sample, HQDbContext>
    {
        #region Sample

        ICollection<Sample> GetSamples();

        Task<ICollection<Sample>> GetSamplesAsync();

        Sample GetSample(string wsno, string factory = "DS");

        Task<Sample> GetSampleAsync(string opcd, string factory = "DS");

        ICollection<Sample> SearchSamples(string keyword);

        Task<ICollection<Sample>> SearchSamplesAsync(string keyword);

        ICollection<Sample> GetSamplesByOp(string opcd);

        Task<ICollection<Sample>> GetSamplesByOpAsync(string opcd);

        ICollection<Sample> GetSamplesByOpStatus(string opcd, string status);

        Task<ICollection<Sample>> GetSamplesByOpStatusAsync(string opcd, string status);

        #endregion
    }
}
