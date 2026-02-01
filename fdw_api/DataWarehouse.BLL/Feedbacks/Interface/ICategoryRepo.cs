using CSG.MI.DTO.Feedback;

namespace CSG.MI.FDW.BLL.Feedbacks.Interface
{
    public interface ICategoryRepo : IDisposable
    {
        ICollection<Category> Get(string lang);

        Task<ICollection<Category>> GetAsync(string lang);
    }
}
