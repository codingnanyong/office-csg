using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Feedback;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Feedbacks.Interface
{
    public interface IFeedbackRepository : IGenericMapRepository<FeedbackEntity, Feedback, HQDbContext>
    {
        #region Feedback - CRUD

        ICollection<Feedback> GetFeedbacks(string lang);

        Task<ICollection<Feedback>> GetFeedbacksAsync(string lang);

        Feedback GetFeedback(int seq, int sys, string lang);

        Task<Feedback> GetFeedbackAsync(int seq, int sys, string lang);

        Feedback CreateFeedback(Feedback feedback);

        Task<Feedback> CreateFeedbackAsync(Feedback feedback);

        Feedback UpdateFeedback(Feedback feedback);

        Task<Feedback> UpdateFeedbackAsync(Feedback feedback);

        int Delete(int seq, int sys);

        Task<int> DeleteAsync(int seq, int sys);

        #endregion
    }
}
