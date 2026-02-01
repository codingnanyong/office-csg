using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart
{
    public class WsWipRepository : GenericMapRepository<WsSummaryEntity, SampleWork, HQDbContext>, IWsWipRepository, IDisposable
    {
        #region Constructors

        public WsWipRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Method - WIP

        public ICollection<Wip> GetWips()
        {
            return Wips();
        }

        public async Task<ICollection<Wip>> GetWipsAsync()
        {
            return await Task.Run(() => Wips());
        }

        public ICollection<Wip> GetByFactory(string factory = "DS")
        {
            var result = Wips().Where(x => x.Factory == factory).ToList();

            return result;
        }

        public async Task<ICollection<Wip>> GetByFactoryAsync(string factory = "DS")
        {
            var result = await Task.Run(() => Wips().Where(x => x.Factory == factory).ToList());

            return result;
        }

        public Wip GetWip(string opcd, string factory = "DS")
        {
            var result = Wips().Where(x => x.Factory == factory && x.OpCd == opcd).FirstOrDefault();

            return result!;
        }

        public async Task<Wip> GetWipAsync(string opcd, string factory = "DS")
        {
            var result = await Task.Run(() => Wips().Where(x => x.Factory == factory && x.OpCd == opcd).FirstOrDefault());

            return result!;
        }

        #endregion

        #region Public Method - WIP : WorkList

        public ICollection<SampleWork> GetWorkList()
        {
            var today = DateTime.Now.ToString("yyyyMMdd");
            var worklist = base.FindBy(x => x.ProdDate == today && x.Status == "I");
            var common = GetCommonDictionary();

            var result = worklist.Select(work =>
            {
                var SeasonName = work?.Season ?? "";
                work!.Season = SeasonNaming(SeasonName, common);

                return _context.Mapper.Map<SampleWork>(work);
            }).ToList();

            return result;
        }

        public async Task<ICollection<SampleWork>> GetWorkListAsync()
        {
            var today = DateTime.Now.ToString("yyyyMMdd");
            var worklist = await base.FindByAsync(x => x.ProdDate == today && x.Status == "I");
            var common = GetCommonDictionary();

            var result = worklist.Select(work =>
            {
                var SeasonName = work?.Season ?? "";
                work!.Season = SeasonNaming(SeasonName, common);

                return _context.Mapper.Map<SampleWork>(work);
            }).ToList();

            return result;
        }

        public ICollection<SampleWork> GetWorkList(string opcd, string factory = "DS")
        {
            var today = DateTime.Now.ToString("yyyyMMdd");
            var worklist = base.FindBy(x => x.ProdDate == today && x.Status == "I" && x.Factory == factory && x.OpCd == opcd);
            var common = GetCommonDictionary();

            var result = worklist.Select(work =>
            {
                var SeasonName = work?.Season ?? "";
                work!.Season = SeasonNaming(SeasonName, common);

                return _context.Mapper.Map<SampleWork>(work);
            }).ToList();

            return result;
        }

        public async Task<ICollection<SampleWork>> GetWorkListAsync(string opcd, string factory = "DS")
        {
            var today = DateTime.Now.ToString("yyyyMMdd");
            var worklist = await base.FindByAsync(x => x.ProdDate == today && x.Status == "I" && x.Factory == factory && x.OpCd == opcd); 
            var common = GetCommonDictionary();

            var result = worklist.Select(work =>
            {
                var SeasonName = work?.Season ?? "";
                work!.Season = SeasonNaming(SeasonName, common);

                return _context.Mapper.Map<SampleWork>(work);
            }).ToList();

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

        #region Private Method - WIP

        private ICollection<Wip> Wips()
        {
            var today = DateTime.Now.ToString("yyyyMMdd");

            var wiplist = _context.wsSummaries.Where(x => x.ProdDate == today && x.Status == "I")
                                              .GroupBy(x => new { x.Factory, x.OpCd, x.OpName, x.OpLocalName })
                                              .Select(prop => new Wip
                                               {
                                                   Factory = prop.Key.Factory,
                                                   OpCd = prop.Key.OpCd,
                                                   OpName = prop.Key.OpName,
                                                   OpLocalName = prop.Key.OpLocalName,
                                                   WipCount = prop.Count() + "(" + Math.Round(prop.Sum(dpp => dpp.ProdQty ?? 0), 0).ToString() + ")"
                                               }).ToList();

            var result = _context.Mapper.Map<ICollection<Wip>>(wiplist);

            return result;
        }

        #endregion

        #region Private Method - WIP : WorkList

        private ICollection<SampleWork> Search(string opcd, string? keyword, string factory = "DS")
        {
            var lowKeyword = keyword?.ToLower();
            var today = DateTime.Now.ToString("yyyyMMdd");
            var worklist = base.FindBy(x => x.ProdDate == today && x.Status == "I" && x.Factory == factory && x.OpCd == opcd);
            var common = GetCommonDictionary();

            var mappedWorklist = worklist.Select(work =>
            {
                var SeasonName = work?.Season ?? "";
                work!.Season = SeasonNaming(SeasonName, common);

                return _context.Mapper.Map<SampleWork>(work);
            });

            var result = string.IsNullOrEmpty(keyword) ? mappedWorklist.ToList(): FilterByKeyword(mappedWorklist, lowKeyword!).ToList();

            return result;
        }

        #endregion

        #region Private Helper Method

        private IDictionary<string, string> GetCommonDictionary()
        {
            return _context.commonMst
                           .Where(x => x.Factory == "DS" && x.ComDiv == "SEASON")
                           .ToDictionary(x => x.ComCode, x => x.ComName);
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