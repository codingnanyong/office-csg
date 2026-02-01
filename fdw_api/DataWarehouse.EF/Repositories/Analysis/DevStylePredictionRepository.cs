using CSG.MI.DTO.Analysis;
using CSG.MI.FDW.EF.Core.Repo;
using Data;
using Entities.Analysis;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Repositories.Analysis.Interface;

namespace Repositories.Analysis
{
    public class DevStylePredictionRepository : GenericMapRepository<DevStylePredictionEntity, DevStylePrediction, HQDbContext>, IDevStylePredictionRepository, IDisposable
    {
        #region Constructor

        public DevStylePredictionRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public ICollection<DevStylePrediction> GetDevPredictions()
        {
            return DevPredictions();
        }

        public async Task<ICollection<DevStylePrediction>> GetDevPredictionsAsync()
        {
            return await Task.Run(() => DevPredictions());
        }

        public DevStylePrediction GetDevPrediction(string style)
        {
            var result = DevPredictions().Where(x => x.DevStyleNumber == style).FirstOrDefault();

            return result!;
        }

        public async Task<DevStylePrediction> GetDevPredictionAsync(string style)
        {
            var result = await Task.Run(() => DevPredictions().Where(x => x.DevStyleNumber == style).FirstOrDefault());

            return result!;
        }

        #endregion

        #region Private Methods - Prediction By DevStyle

        private ICollection<DevStylePrediction> DevPredictions()
        {
            var entities = _context.devPredictions.ToList();

            var result = entities.GroupBy(e => e.DevStyleNumber)
                                 .Select(group => new DevStylePrediction
                                 {
                                     DevStyleNumber = group.Key,
                                     Routes = group.Select(e => new PredictionRoute
                                     {
                                         TagType = e.TagType,
                                         OpCode = e.OpCode
                                     }).ToList()
                                 })
                                 .OrderBy(x => x.DevStyleNumber)
                                 .ToList();
            return result;
        }

        #endregion
    }
}
