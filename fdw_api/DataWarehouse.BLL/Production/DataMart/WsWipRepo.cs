using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;

namespace CSG.MI.FDW.BLL.Production.DataMart
{
    public class WsWipRepo : IWsWipRepo
    {
        #region Constructors

        private IWsWipRepository _repo;

        public WsWipRepo(IWsWipRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region WIP

        public ICollection<Wip> GetAll()
        {
            return _repo.GetWips();
        }

        public async Task<ICollection<Wip>> GetAllAsync()
        {
            return await _repo.GetWipsAsync();
        }

        public ICollection<Wip> GetByFactory(string factory = "DS")
        {
            return _repo.GetByFactory(factory);
        }

        public async Task<ICollection<Wip>> GetByFactoryAsync(string factory = "DS")
        {
            return await _repo.GetByFactoryAsync(factory);
        }

        public Wip GetWip(string opcd, string factory = "DS")
        {
            return _repo.GetWip(opcd, factory);
        }

        public async Task<Wip> GetWipAsync(string opcd, string factory = "DS")
        {
            return await _repo.GetWipAsync(opcd, factory);
        }

        #endregion

        #region WIP/Prod WorkList

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
