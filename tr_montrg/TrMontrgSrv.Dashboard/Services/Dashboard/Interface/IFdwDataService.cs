using CSG.MI.DTO.Feedback;

namespace CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard.Interface
{
    public interface IFdwDataService
    {
        Task<ICollection<Category>> GetCategories(string lang);

        Task SendRequest(Feedback feedback);
    }
}
