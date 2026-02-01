using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;

namespace CSG.MI.FDW.BLL.Production.DataMart
{
    public class WsSampleRepo : IWsSampleRepo
    {
        #region Constructors

        private IWsSampleRepository _repo;

        public WsSampleRepo(IWsSampleRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Sample

        public ICollection<Sample> GetSamples()
        {
            return _repo.GetSamples();
        }

        public async Task<ICollection<Sample>> GetSamplesAsync()
        {
            return await _repo.GetSamplesAsync();
        }

        public Sample GetSample(string wsno, string factory = "DS")
        {
            return _repo.GetSample(wsno, factory);
        }

        public async Task<Sample> GetSampleAsync(string wsno, string factory = "DS")
        {
            return await _repo.GetSampleAsync(wsno, factory);
        }

        public ICollection<Sample> SearchSamples(string keyword)
        {
            return _repo.SearchSamples(keyword);
        }

        public async Task<ICollection<Sample>> SearchSamplesAsync(string keyword)
        {
            return await _repo.SearchSamplesAsync(keyword);
        }

        public ICollection<Sample> GetSamplesByOp(string opcd)
        {
            return _repo.GetSamplesByOp(opcd);
        }

        public Task<ICollection<Sample>> GetSamplesByOpAsync(string opcd)
        {
            return _repo.GetSamplesByOpAsync(opcd);
        }

        public ICollection<Sample> GetSamplesByOpStatus(string opcd, string status)
        {
            return _repo.GetSamplesByOpStatus(opcd, status);
        }

        public Task<ICollection<Sample>> GetSamplesByOpStatusAsync(string opcd, string status)
        {
            return _repo.GetSamplesByOpStatusAsync(opcd, status);
        }

        #endregion

        #region Dispose

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_repo != null)
                {
                    _repo.Dispose();
                    _repo = null!;
                }
            }
        }
        #endregion
    }
}
