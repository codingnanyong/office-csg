using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart
{
    public class DailyStatusRepository : GenericMapRepository<DailyStatusEntity, DailyStatus, HQDbContext>, IDailyStatusRepository, IDisposable
    {
        #region Constructors

        public DailyStatusRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods - DailyStatus

        public ICollection<DailyStatus> GetDailyStatus()
        {
            return base.GetAll();
        }

        public async Task<ICollection<DailyStatus>> GetDailyStatusAsync()
        {
            return await base.GetAllAsync();
        }

        public ICollection<DailyStatus> GetDailyStatusByFactory(string factory = "DS")
        {
            return base.FindBy(x=> x.Factory == factory);
        }

        public async Task<ICollection<DailyStatus>> GetDailyStatusByFactoryAsync(string factory = "DS")
        {
            return await base.FindByAsync(x => x.Factory == factory);
        }

        public DailyStatus GetDailyStatus(string opcd, string factory = "DS")
        {
            return base.Find(x => x.Factory == factory && x.OpCd == opcd);
        }

        public async Task<DailyStatus> GetDailyStatusAsync(string opcd, string factory = "DS")
        {
            return await base.FindAsync(x => x.Factory == factory && x.OpCd == opcd);
        }

        #endregion

        #region Public Methods - DailyStatus : WorkList

        public ICollection<SampleWork> GetWorkList()
        {
            return WorkList();
        }

        public async Task<ICollection<SampleWork>> GetWorkListAsync()
        {
            return await Task.Run(() => WorkList());
        }

        public ICollection<SampleWork> GetWorkList(string opcd, string factory = "DS")
        {
           var result = WorkList().Where(x => x.Factory == factory && x.OpCd == opcd).ToList();

           return result;
        }

        public async Task<ICollection<SampleWork>> GetWorkListAsync(string opcd, string factory = "DS")
        {
            var result = await Task.Run(() => WorkList().Where(x => x.Factory == factory && x.OpCd == opcd).ToList());

            return result;
        }

        public ICollection<SampleWork> GetSearch(string opcd, string? keyword, string factory = "DS")
        {
           return Search(opcd, keyword, factory);
        }

        public async Task<ICollection<SampleWork>> GetSearchAsync(string opcd, string? keyword, string factory = "DS")
        {
            return await Task.Run(() => Search(opcd, keyword, factory));
        }

        #endregion

        #region Private Method - DailySatus : WorkList

        private ICollection<SampleWork> WorkList()
        {
            var today = DateTime.Now.ToString("yyyyMMdd");
            var common = GetCommonDictionary();

            var worklist = _context.wsSummaries.Where(x => (x.PlanDate == today || x.ProdDate == today))
                                               .OrderBy(x => x.Status).ToList();

            var result = worklist.Select(work =>
            {
                var SeasonName = work?.Season ?? "";
                
                work!.Season = SeasonNaming(SeasonName, common);

                return _context.Mapper.Map<SampleWork>(work);
            }).ToList();

            return result;
        }

        private ICollection<SampleWork> Search(string opcd, string? keyword, string factory = "DS")
        {
            var lowKeyword = keyword?.ToLower();
            var worklist = WorkList().Where(x => x.Factory == factory && x.OpCd == opcd);

            var result = string.IsNullOrEmpty(keyword)? MapToSampleWork(worklist): FilterByKeyword(worklist, lowKeyword!).ToList();

            return result;
        }

        #endregion

        #region Private Helper Method

        private IDictionary<string, string> GetCommonDictionary()
        {
            return _context.commonMst.Where(x => x.Factory == "DS" && x.ComDiv == "SEASON").ToDictionary(x => x.ComCode, x => x.ComName);
        }

        private string SeasonNaming(string detailSeason, IDictionary<string, string> common)
        {
            var seasonCode = detailSeason.Length >= 6 ? detailSeason.Substring(4, 2) : "";
            var seasonPrefix = detailSeason.Length >= 4 ? detailSeason.Substring(2, 2) : "";

            if (common.TryGetValue(seasonCode, out var seasonName))
            {
                seasonName += seasonPrefix;
            }
            else
            {
                seasonName = seasonPrefix;
            }

            return seasonName;
        }

        private IEnumerable<SampleWork> FilterByKeyword(IEnumerable<SampleWork> worklist, string lowKeyword)
        {
            return worklist.Where(x => IsMatch(x, lowKeyword));
        }

        private bool IsMatch(SampleWork sample, string lowKeyword)
        {
            var stringProperties = new List<Func<SampleWork, string?>>()
            {
                x => x.WsNo,
                x => x.Pm,
                x => x.PmName,
                x => x.Model,
                x => x.Season,
                x => x.BomId,
                x => x.StyleCd
            };

            return stringProperties.Any(selector =>
            {
                var value = selector(sample);
                return !string.IsNullOrEmpty(value) && value.ToLower().Contains(lowKeyword);
            });
        }

        private ICollection<SampleWork> MapToSampleWork(IEnumerable<SampleWork> worklist)
        {
            return worklist.Select(work => _context.Mapper.Map<SampleWork>(work)).ToList();
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