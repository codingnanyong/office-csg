using CSG.MI.DTO.Feedback;

namespace CSG.MI.FDW.BLL.Feedbacks.Interface
{
    public interface IFeedbackRepo : IDisposable
    {
        ICollection<Feedback> GetFeedbacks(string lang);

        Task<ICollection<Feedback>> GetFeedbacksAsync(string lang);

        Feedback GetFeedback(int id, int sys, string lang);

        Task<Feedback> GetFeedbackAsync(int id, int sys, string lang);

        Feedback CreateFeedback(Feedback feedback);

        Task<Feedback> CreateFeedbackAsync(Feedback feedback);

        Feedback UpdateFeedback(Feedback feedback);

        Task<Feedback> UpdateFeedbackAsync(Feedback feedback);

        int Delete(int seq, int sys);

        Task<int> DeleteAsync(int seq, int sys);
    }
}
