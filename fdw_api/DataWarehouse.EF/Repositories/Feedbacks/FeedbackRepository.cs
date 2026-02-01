using Microsoft.EntityFrameworkCore;
using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.Feedback;
using CSG.MI.FDW.EF.Repositories.Feedbacks.Interface;
using Data;

namespace CSG.MI.FDW.EF.Repositories.Feedbacks
{
    public class FeedbackRepository : GenericMapRepository<FeedbackEntity, Feedback, HQDbContext>, IFeedbackRepository, IDisposable
    {
        #region Constructors

        public FeedbackRepository(HQDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Feedback CRUD - Create

        public Feedback CreateFeedback(Feedback feedback)
        {
            return base.Add(feedback);
        }

        public async Task<Feedback> CreateFeedbackAsync(Feedback feedback)
        {
            return await base.AddAsync(feedback);
        }

        #endregion

        #region Feedback CRUD - Read

        public ICollection<Feedback> GetFeedbacks(string lang)
        {
            return SetFeedbacks(lang);
        }

        public async Task<ICollection<Feedback>> GetFeedbacksAsync(string lang)
        {
            return await Task.Run(() => SetFeedbacks(lang));
        }

        public Feedback GetFeedback(int seq, int sys, string lang)
        {
            return SetFeedback(seq, sys, lang);
        }

        public async Task<Feedback> GetFeedbackAsync(int seq, int sys, string lang)
        {
            return await Task.Run(() => SetFeedback(seq, sys, lang));
        }

        #endregion

        #region Feedback CRUD - Update

        public Feedback UpdateFeedback(Feedback feedback)
        {
            return base.Update(feedback, feedback.Seq!);
        }

        public async Task<Feedback> UpdateFeedbackAsync(Feedback feedback)
        {
            return await base.UpdateAsync(feedback, feedback.Seq!);
        }

        #endregion

        #region Feedback CRUD - Delete

        public int Delete(int seq, int sys)
        {
            var item = _context.feedbacks.Where(x => x.Seq == seq && x.System == sys).ToList();

            if (!item.Any())
                return 0;

            _context.feedbacks.RemoveRange(item);

            return Save();
        }

        public async Task<int> DeleteAsync(int seq, int sys)
        {
            var item = await _context.feedbacks.Where(x => x.Seq == seq && x.System == sys)
                                                 .ToListAsync();

            if (!item.Any())
                return 0;

            _context.feedbacks.RemoveRange(item);

            return await SaveAsync();
        }

        #endregion

        #region Private Method

        private ICollection<Feedback> SetFeedbacks(string lang)
        {
            var feedbacks = from fbs in _context.feedbacks
                            join cat in _context.categories on fbs.Category equals cat.Key
                            join sys in _context.systems on fbs.System equals sys.Id
                            select new Feedback
                            {
                                Seq = fbs.Seq,
                                System = sys.Name,
                                Category = GetTranslatedValue(cat, lang),
                                Comment = fbs.Comment,
                                EditDate = fbs.EditDate
                            };

            return feedbacks.ToList();
        }

        private Feedback SetFeedback(int seq, int sys, string lang)
        {
            var feedbacks = (from fbs in _context.feedbacks
                             join cat in _context.categories on fbs.Category equals cat.Key
                             join _sys in _context.systems on fbs.System equals _sys.Id
                             where fbs.Seq == seq && _sys.Id == sys
                             select new Feedback
                             {
                                 Seq = fbs.Seq,
                                 System = _sys.Name,
                                 Category = GetTranslatedValue(cat, lang),
                                 Comment = fbs.Comment,
                                 EditDate = fbs.EditDate
                             }).FirstOrDefault();

            return feedbacks!;
        }

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
