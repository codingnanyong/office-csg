using CSG.MI.DTO.Production;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Repositories.Production.DataMart
{
    public class WsSampleRepository : GenericMapRepository<WsEntity, Sample, HQDbContext>, IWsSampleRepository, IDisposable
    {
        #region Constructors

        public WsSampleRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Method - Sample (Include : Detail,History,Coordinate)

        public ICollection<Sample> GetSamples()
        {
            return Samples();
        }

        public async Task<ICollection<Sample>> GetSamplesAsync()
        {
            return await Task.Run(() => Samples());
        }

        public Sample GetSample(string wsno, string factory = "DS")
        {
            var samples = Samples();

            var result = samples.Where(x => x.WsNo == wsno && x.Factory == factory).FirstOrDefault();

            return result!;
        }

        public async Task<Sample> GetSampleAsync(string wsno, string factory = "DS")
        {
            var samples = await Task.Run(() => Samples());

            var result = samples.Where(x => x.WsNo == wsno && x.Factory == factory).FirstOrDefault();

            return result!;
        }

        public ICollection<Sample> SearchSamples(string keyword)
        {
            return SrchSamples(keyword);
        }

        public async Task<ICollection<Sample>> SearchSamplesAsync(string keyword)
        {
            return await Task.Run(() => SrchSamples(keyword));
        }

        public ICollection<Sample> GetSamplesByOp(string opcd)
        {
            return SamplesByOp(opcd);
        }

        public async Task<ICollection<Sample>> GetSamplesByOpAsync(string opcd)
        {
            return await Task.Run(() => SamplesByOp(opcd));
        }

        public ICollection<Sample> GetSamplesByOpStatus(string opcd,string status)
        {
            return SamplesByOpStatus(opcd, status);
        }

        public async Task<ICollection<Sample>> GetSamplesByOpStatusAsync(string opcd, string status)
        {
            return await Task.Run(() => SamplesByOpStatus(opcd,status));
        }

        #endregion

        #region Private Method - Sample (Include : Detail,History,Coordinate)

        private ICollection<Sample> Samples()
        {
            var wsEntities = GetEntities(_context.ws);
            var details = GetEntities(_context.wsDetails);
            var histories = GetEntities(_context.wsHistories);
            var coordinates = GetEntities(_context.wsCoodinates);
            var common = GetCommonDictionary();

            var result = wsEntities.Select(ws =>
            {
                var sample = _context.Mapper.Map<Sample>(ws);

                sample.Detail = _context.Mapper.Map<SampleDetail>(details.FirstOrDefault(detail => detail.WsNo == ws.WsNo));

                var SeasonName = sample.Detail?.Season ?? "";
                sample.Detail!.Season = SeasonNaming(SeasonName, common);

                sample.Histories = histories.Where(his => his.WsNo == ws.WsNo)
                                            .Select(his => _context.Mapper.Map<SampleHist>(his))
                                            .ToList();

                sample.Coordinates = coordinates.Where(coor => coor.WsNo == ws.WsNo)
                                                .Select(coor => _context.Mapper.Map<SampleCoordinate>(coor))
                                                .ToList();

                return sample;
            }).ToList();

            return result;
        }

        private ICollection<Sample> SrchSamples(string keyword)
        {
            var lowKeyword = keyword?.ToLower();
            var samples = Samples();

            if (string.IsNullOrEmpty(keyword))
            {
                return samples;
            }

            var result = samples.Where(sample =>IsMatch(sample, lowKeyword!) ||
                                       sample.Coordinates.Any(coord =>IsCoordinateMatch(coord, lowKeyword!)))
                                .ToList();

            return result;
        }

        private ICollection<Sample> SamplesByOp(string opcd)
        {
            return GetSamplesByCondition(query => query.Where(x => x.opcd == opcd));
        }

        private ICollection<Sample> SamplesByOpStatus(string opcd, string status)
        {
            return GetSamplesByCondition(query => query.Where(x => x.opcd == opcd && x.Status == status));
        }

        #endregion

        #region Private Helper Method

        private List<T> GetEntities<T>(DbSet<T> dbSet) where T : class
        {
            return dbSet.ToList();
        }

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

        private bool IsMatch(Sample sample, string lowKeyword)
        {
            var stringProperties = new List<Func<Sample, string?>>()
            {
                sample => sample.WsNo,
                sample => sample.Detail?.DevColorwayId,
                sample => sample.Detail?.ColorVer,
                sample => sample.Detail?.ModelId,
                sample => sample.Detail?.DevName,
                sample => sample.Detail?.DevStyleNumber,
                sample => sample.Detail?.SampleEts,
                sample => sample.Detail?.Category,
                sample => sample.Detail?.Season,
                sample => sample.Detail?.StCd,
                sample => sample.Detail?.SubStCd,
                sample => sample.Detail?.PM,
                sample => sample.Detail?.PmName,
                sample => sample.Detail?.StyleCd
            };

            return stringProperties.Any(selector =>
            {
                var value = selector(sample);
                return !string.IsNullOrEmpty(value) && value!.ToLower().Contains(lowKeyword);
            });
        }

        private bool IsCoordinateMatch(SampleCoordinate coord, string lowKeyword)
        {
            return (!string.IsNullOrEmpty(coord.tagId) && coord.tagId.ToLower().Contains(lowKeyword)) ||
                   (!string.IsNullOrEmpty(coord.Floor) && coord.Floor.ToLower().Contains(lowKeyword));
        }

        private ICollection<Sample> GetSamplesByCondition(Func<IQueryable<WsCoodinateEntity>, IQueryable<WsCoodinateEntity>> conditionQuery)
        {
            var coordinates = conditionQuery(_context.wsCoodinates).Select(y => y.WsNo).ToList();

            var result = new List<Sample>();

            foreach (var wsNo in coordinates)
            {
                var sample = Samples().FirstOrDefault(x => x.WsNo == wsNo);

                if (sample != null)
                {
                    result.Add(sample);
                }
            }
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