using CSG.MI.DTO.Analysis;

namespace CSG.MI.FDW.BLL.Analysis.Interface
{
    public interface IDevStylePredictionRepo : IDisposable
    {
        public ICollection<DevStylePrediction> GetDevPredictions();

        public Task<ICollection<DevStylePrediction>> GetDevPredictionsAsync();

        public DevStylePrediction GetDevPrediction(string style);

        public Task<DevStylePrediction> GetDevPredictionAsync(string style);
    }
}
