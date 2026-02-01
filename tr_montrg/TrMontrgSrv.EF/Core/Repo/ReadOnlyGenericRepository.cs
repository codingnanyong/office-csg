using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Core.Repo
{
    public class ReadOnlyGenericRepository<T> : IReadOnlyGenericRepository<T> where T : class
    {
        #region Fields

        protected readonly AppDbContext _context;

        #endregion

        #region Constructors

        public ReadOnlyGenericRepository(AppDbContext ctx)
        {
            _context = ctx;
        }

        #endregion

        #region Properties

        public AppDbContext Context => _context;

        protected bool IsDisposed { get; private set; }

        #endregion

        #region Public Methods

        #region Count

        public int Count()
        {
            return _context.Set<T>().Count();
        }

        public async Task<int> CountAsync()
        {
            return await _context.Set<T>().CountAsync();
        }

        #endregion

        #region Get

        public virtual T Get(params object[] keys)
        {
            return _context.Set<T>().Find(keys);
        }

        public virtual async Task<T> GetAsync(params object[] keys)
        {
            return await _context.Set<T>().FindAsync(keys);
        }

        public IQueryable<T> GetAll()
        {
            return _context.Set<T>();
        }

        public virtual async Task<ICollection<T>> GetAllAsync()
        {
            return await _context.Set<T>().ToListAsync();
        }

        public IQueryable<T> GetAllIncluding(params Expression<Func<T, object>>[] includeProperties)
        {
            IQueryable<T> queryable = GetAll();
            foreach (Expression<Func<T, object>> includeProperty in includeProperties)
            {
                queryable = queryable.Include<T, object>(includeProperty);
            }

            return queryable;
        }

        #endregion

        #region Find

        public virtual T Find(Expression<Func<T, bool>> match)
        {
            return _context.Set<T>().SingleOrDefault(match);
        }

        public virtual async Task<T> FindAsync(Expression<Func<T, bool>> match)
        {
            return await _context.Set<T>().SingleOrDefaultAsync(match);
        }

        public virtual IQueryable<T> FindBy(Expression<Func<T, bool>> predicate)
        {
            IQueryable<T> query = _context.Set<T>().Where(predicate);
            return query;
        }

        public virtual async Task<ICollection<T>> FindByAsync(Expression<Func<T, bool>> predicate)
        {
            return await _context.Set<T>().Where(predicate).ToListAsync();
        }

        public ICollection<T> FindAll(Expression<Func<T, bool>> match)
        {
            return _context.Set<T>().Where(match).ToList();
        }

        public async Task<ICollection<T>> FindAllAsync(Expression<Func<T, bool>> match)
        {
            return await _context.Set<T>().Where(match).ToListAsync();
        }

        #endregion

        #endregion // Public Methods

        #region Disposable

        protected virtual void Dispose(bool disposing)
        {
            if (IsDisposed)
                return;

            // Dispose managed objects.
            if (disposing)
            {
                //_context?.Dispose();
            }

            // Free unmanaged resources and override a finalizer below.
            // Set large fields to null.

            IsDisposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            //GC.SuppressFinalize(this);
        }

        #endregion
    }
}
