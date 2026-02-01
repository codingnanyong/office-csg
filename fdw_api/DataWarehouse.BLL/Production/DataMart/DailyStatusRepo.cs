using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;

namespace CSG.MI.FDW.BLL.Production.DataMart
{
    public class DailyStatusRepo : IDailyStatusRepo
    {
        #region Constructors

        private IDailyStatusRepository _repo;

        public DailyStatusRepo(IDailyStatusRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region DailyStatus

        public ICollection<DailyStatus> GetDailyStatus()
        {
            return _repo.GetDailyStatus();
        }

        public async Task<ICollection<DailyStatus>> GetDailyStatusAsync()
        {
            return await _repo.GetDailyStatusAsync();
        }

        public ICollection<DailyStatus> GetDailyStatusByFactory(string factory = "DS")
        {
            return _repo.GetDailyStatusByFactory(factory);
        }

        public async Task<ICollection<DailyStatus>> GetDailyStatusByFactoryAsync(string factory = "DS")
        {
            return await _repo.GetDailyStatusByFactoryAsync(factory);
        }

        public DailyStatus GetDailyStatus(string opcd, string factory = "DS")
        {
            return _repo.GetDailyStatus(opcd, factory);
        }

        public async Task<DailyStatus> GetDailyStatusAsync(string opcd, string factory = "DS")
        {
            return await _repo.GetDailyStatusAsync(opcd, factory);
        }

        #endregion

        #region DailyStatus/Plan & Prod WorkList

        public ICollection<SampleWork> GetWorkList()
        {
            return _repo.GetWorkList();
        }

        public async Task<ICollection<SampleWork>> GetWorkListAsync()
        {
            return await _repo.GetWorkListAsync();
        }

        public ICollection<SampleWork> GetWorkList(string opcd, string factory = "DS")
        {
            return _repo.GetWorkList(opcd, factory);
        }

        public async Task<ICollection<SampleWork>> GetWorkListAsync(string opcd, string factory = "DS")
        {
            return await _repo.GetWorkListAsync(opcd, factory);
        }

        public ICollection<SampleWork> GetSearch(string opcd, string? keyword, string factory = "DS")
        {
            return _repo.GetSearch(opcd, keyword, factory);
        }

        public async Task<ICollection<SampleWork>> GetSearchAsync(string opcd, string? keyword, string factory = "DS")
        {
            return await _repo.GetSearchAsync(opcd, keyword, factory);
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
