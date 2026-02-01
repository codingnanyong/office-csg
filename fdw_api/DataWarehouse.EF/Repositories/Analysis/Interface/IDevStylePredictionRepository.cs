using CSG.MI.DTO.Analysis;
using CSG.MI.FDW.EF.Core.Repo;
using Data;
using Entities.Analysis;

namespace Repositories.Analysis.Interface
{
    public interface IDevStylePredictionRepository : IGenericMapRepository<DevStylePredictionEntity, DevStylePrediction, HQDbContext>
    {
        public ICollection<DevStylePrediction> GetDevPredictions();

        public Task<ICollection<DevStylePrediction>> GetDevPredictionsAsync();

        public DevStylePrediction GetDevPrediction(string style);

        public Task<DevStylePrediction> GetDevPredictionAsync(string style);
    }
}
