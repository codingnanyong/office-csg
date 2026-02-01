using CSG.MI.DAO.Feedback;
using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Feedback;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Feedbacks.Interface
{
    public interface ICategoryRepository : IGenericMapRepository<CategoryEntity, CategoryMst, HQDbContext>
    {
        ICollection<Category> Get(string lang);

        Task<ICollection<Category>> GetAsync(string lang);
    }
}
