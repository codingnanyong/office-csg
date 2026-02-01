using Data;
using Microsoft.EntityFrameworkCore;
using System.Linq.Expressions;

namespace CSG.MI.FDW.EF.Core.Repo
{
    public class ReadOnlyGenericRepository<T> : IReadOnlyGenericRepository<T> where T : class
	{
		#region Fields

		protected readonly HQDbContext _context;

		#endregion

		#region Constructors

		public ReadOnlyGenericRepository(HQDbContext ctx)
		{
			_context = ctx;
		}

		#endregion

		#region Properties

		public HQDbContext Context => _context;

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
			T? entity = _context.Set<T>().Find(keys);
			if (entity == null)
			{
				throw new Exception("Entity not found.");
			}
			return entity;
		}

		public virtual async Task<T> GetAsync(params object[] keys)
		{
			return await _context.Set<T>().FindAsync(keys) ?? throw new InvalidOperationException("Entity not found.");
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
			return _context.Set<T>().SingleOrDefault(match)!;
		}

		public virtual async Task<T> FindAsync(Expression<Func<T, bool>> match)
		{
            /*return await _context.Set<T>().SingleOrDefaultAsync(match);*/
            var entity = await _context.Set<T>().SingleOrDefaultAsync(match);
            if (entity == null)
            {
                throw new Exception($"Entity of type {typeof(T)} with specified condition not found.");
            }
            return entity!;
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
