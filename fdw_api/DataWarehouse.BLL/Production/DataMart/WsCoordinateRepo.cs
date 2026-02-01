using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;

namespace CSG.MI.FDW.BLL.Production.DataMart
{
    public class WsCoordinateRepo : IWsCoordinateRepo
    {
        #region Constructors

        private IWsCoordinateRepository _repo;

        public WsCoordinateRepo(IWsCoordinateRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Methods - Coordinates

        public ICollection<SampleCoordinate> GetCoordinates()
        {
            return _repo.GetCoordinates();
        }

        public async Task<ICollection<SampleCoordinate>> GetCoordinatesAsync()
        {
            return await _repo.GetCoordinatesAsync();
        }

        #endregion

        #region Public Methods - Coordinates History

        public ICollection<SampleCoordinateHistory> GetHistory()
        {
            return _repo.GetHistory();
        }

        public async Task<ICollection<SampleCoordinateHistory>> GetHistoryAsync()
        {
           return await _repo.GetHistoryAsync();
        }

        public ICollection<SampleCoordinateHistory> GetHistoryBy(string wsno)
        {
           return _repo.GetHistoryBy(wsno);
        }

        public async Task<ICollection<SampleCoordinateHistory>> GetHistoryByAsync(string wsno)
        {
            return await _repo.GetHistoryByAsync(wsno);
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
