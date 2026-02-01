using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart
{
    public class WsCoordinateRepository : GenericMapRepository<WsCoodinateEntity, SampleCoordinate, HQDbContext>, IWsCoordinateRepository, IDisposable
    {
        #region Constructor

        public WsCoordinateRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods - Coordinates

        public ICollection<SampleCoordinate> GetCoordinates()
        {
            return base.GetAll();
        }

        public async Task<ICollection<SampleCoordinate>> GetCoordinatesAsync()
        {
            return await base.GetAllAsync();
        }

        #endregion

        #region Public Methods - Coordinates History

        public ICollection<SampleCoordinateHistory> GetHistory()
        {
            return CoordinateHistories();
        }

        public async Task<ICollection<SampleCoordinateHistory>> GetHistoryAsync()
        {
            return await Task.Run(() => CoordinateHistories());
        }

        public ICollection<SampleCoordinateHistory> GetHistoryBy(string wsno)
        {
            var histores = CoordinateHistories();

            var result = histores.Where(x => x.WsNo == wsno).ToList();

            return result;
        }

        public async Task<ICollection<SampleCoordinateHistory>> GetHistoryByAsync(string wsno)
        {
            var histores = await Task.Run(() => CoordinateHistories());

            var result = histores.Where(x => x.WsNo == wsno).ToList();

            return result;
        }

        #endregion

        #region Private Methods - Coordinates History

        private ICollection<SampleCoordinateHistory> CoordinateHistories()
        {
            var wsEntities = _context.ws.ToList();
            var coord_his = _context.wsCoordinateHistories.ToList();

            var result = wsEntities.Select( ws => new SampleCoordinateHistory
            {
                WsNo = ws.WsNo,
                Histories = coord_his.Where(x => x.WsNo == ws.WsNo)
                                     .Select(his => _context.Mapper.Map<CoordinateHistory>(his))
                                     .ToList()
            }).ToList();

            return result;
        }

        #endregion

        #region Disposable

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed == false)
            {
                if (disposing)
                {
                }
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
