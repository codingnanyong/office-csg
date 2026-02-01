using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.BLL.Feedbacks.Interface;
using CSG.MI.FDW.EF.Repositories.Feedbacks.Interface;

namespace CSG.MI.FDW.BLL.Feedbacks
{
    public class FeedbackRepo : IFeedbackRepo
    {
        #region Constructors

        private IFeedbackRepository _repo;

        public FeedbackRepo(IFeedbackRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Feedback CRUD - Create

        public Feedback CreateFeedback(Feedback feedback)
        {
            return _repo.CreateFeedback(feedback);
        }

        public Task<Feedback> CreateFeedbackAsync(Feedback feedback)
        {
            return _repo.CreateFeedbackAsync(feedback);
        }

        #endregion

        #region Feedback CRUD - Read

        public ICollection<Feedback> GetFeedbacks(string lang)
        {
            return _repo.GetFeedbacks(lang);
        }

        public async Task<ICollection<Feedback>> GetFeedbacksAsync(string lang)
        {
            return await _repo.GetFeedbacksAsync(lang);
        }

        public Feedback GetFeedback(int seq, int sys, string lang)
        {
            return _repo.GetFeedback(seq, sys, lang);
        }

        public async Task<Feedback> GetFeedbackAsync(int seq, int sys, string lang)
        {
            return await Task.Run(() => _repo.GetFeedbackAsync(seq, sys, lang));
        }

        #endregion

        #region Feedback CRUD - Update

        public Feedback UpdateFeedback(Feedback feedback)
        {
            return _repo.UpdateFeedback(feedback);
        }

        public async Task<Feedback> UpdateFeedbackAsync(Feedback feedback)
        {
            return await _repo.UpdateFeedbackAsync(feedback);
        }

        #endregion

        #region Feedback CRUD - Delete

        public int Delete(int seq, int sys)
        {
            return _repo.Delete(seq, sys);
        }

        public Task<int> DeleteAsync(int seq, int sys)
        {
            return _repo.DeleteAsync(seq, sys);
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
