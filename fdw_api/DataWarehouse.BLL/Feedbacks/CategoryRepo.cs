using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.BLL.Feedbacks.Interface;
using CSG.MI.FDW.EF.Repositories.Feedbacks.Interface;

namespace CSG.MI.FDW.BLL.Feedbacks
{
    public class CategoryRepo : ICategoryRepo
    {
        #region Constructors

        private ICategoryRepository _repo;

        public CategoryRepo(ICategoryRepository repo)
        {
            _repo = repo;
        }

        #endregion

        public ICollection<Category> Get(string lang)
        {
            return _repo.Get(lang);
        }

        public Task<ICollection<Category>> GetAsync(string lang)
        {
            return _repo.GetAsync(lang);
        }

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
