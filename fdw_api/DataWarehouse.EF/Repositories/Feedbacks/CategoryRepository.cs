using CSG.MI.DAO.Feedback;
using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Feedback;
using CSG.MI.FDW.EF.Repositories.Feedbacks.Interface;
using Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Repositories.Feedbacks
{
    public class CategoryRepository : GenericMapRepository<CategoryEntity, CategoryMst, HQDbContext>, ICategoryRepository, IDisposable
    {
        #region Constructor

        public CategoryRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public ICollection<Category> Get(string lang)
        {
            var categories = from cats in _context.categories
                             orderby cats.Key
                             select new Category
                             {
                                 Id = cats.Key,
                                 Value = GetTranslatedValue(cats, lang)
                             };

            if (!IsSupportedLanguage(lang))
            {
                return null!;
            }

            return categories.ToList();
        }

        public async Task<ICollection<Category>> GetAsync(string lang)
        {
            var categories = from cats in _context.categories
                             orderby cats.Key
                             select new Category
                             {
                                 Id = cats.Key,
                                 Value = GetTranslatedValue(cats, lang)
                             };

            if (!IsSupportedLanguage(lang))
            {
                return null!;
            }

            return await categories.ToListAsync();
        }

        #endregion

        #region Private Methods

        private static string GetTranslatedValue(CategoryEntity cat, string lang)
        {
            switch (lang.ToLower())
            {
                case "eng":
                case "en":
                    return cat.Value_Eng;
                case "kor":
                case "kr":
                    return cat.Value_Kor;
                default:
                    return null!;
            }
        }

        private static bool IsSupportedLanguage(string lang)
        {
            switch (lang.ToLower())
            {
                case "en":
                case "eng":
                case "kr":
                case "kor":
                    return true;
                default:
                    return false;
            }
        }

        #endregion

        #region Disposable

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed == false)
            {
                if (disposing)
                {
                }
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
