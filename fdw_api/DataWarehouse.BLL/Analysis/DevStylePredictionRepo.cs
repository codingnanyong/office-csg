using CSG.MI.DTO.Analysis;
using CSG.MI.FDW.BLL.Analysis.Interface;
using Repositories.Analysis.Interface;

namespace CSG.MI.FDW.BLL.Analysis
{
    public class DevStylePredictionRepo : IDevStylePredictionRepo
    {
        #region Constructors

        private IDevStylePredictionRepository _repo;

        public DevStylePredictionRepo(IDevStylePredictionRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Methods

        public ICollection<DevStylePrediction> GetDevPredictions()
        {
            return _repo.GetDevPredictions();
        }

        public Task<ICollection<DevStylePrediction>> GetDevPredictionsAsync()
        {
            return _repo.GetDevPredictionsAsync();
        }

        public DevStylePrediction GetDevPrediction(string style)
        {
            return _repo.GetDevPrediction(style);
        }

        public async Task<DevStylePrediction> GetDevPredictionAsync(string style)
        {
            return await _repo.GetDevPredictionAsync(style);
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
