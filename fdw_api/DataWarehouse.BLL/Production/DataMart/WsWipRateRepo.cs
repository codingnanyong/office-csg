using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;

namespace CSG.MI.FDW.BLL.Production.DataMart
{
    public class WsWipRateRepo : IWsWipRateRepo
    {
        #region Constructors

        private IWsWipRateRepository _repo;

        public WsWipRateRepo(IWsWipRateRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region WIP Rate

        public ICollection<WipRate> GetWipRates()
        {
            return _repo.GetWipRates();
        }

        public async Task<ICollection<WipRate>> GetWipRatesAsync()
        {
            return await _repo.GetWipRatesAsync();
        }

        public WipRate GetWipRate(string opcd)
        {
            return _repo.GetWipRate(opcd);
        }

        public async Task<WipRate> GetWipRateAsync(string opcd)
        {
            return await _repo.GetWipRateAsync(opcd);
        }

        #endregion

        #region WIP Rate : WS List

        public ICollection<SampleWork> GetSampleKeys(string opcd, string status)
        {
            return _repo.GetSampleKeys(opcd, status);
        }

        public async Task<ICollection<SampleWork>> GetSampleKeysAsync(string opcd, string status)
        {
            return await _repo.GetSampleKeysAsync(opcd, status);
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
