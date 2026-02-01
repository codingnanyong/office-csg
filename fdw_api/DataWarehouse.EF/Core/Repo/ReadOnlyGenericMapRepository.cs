using AutoMapper.QueryableExtensions;
using CSG.MI.FDW.EF.Data;
using Microsoft.EntityFrameworkCore;
using System.Diagnostics;
using System.Linq.Expressions;

namespace CSG.MI.FDW.EF.Core.Repo
{
	public class ReadOnlyGenericMapRepository<E, M, C> : IReadOnlyGenericMapRepository<E, M> where E : class
																							 where M : class
																						     where C : BaseDbContext
	{
		#region Fields

		protected readonly C _context;

		#endregion

		#region Constructors

		public ReadOnlyGenericMapRepository(C ctx)
		{
			_context = ctx;
		}

		#endregion

		#region Properties

		protected bool IsDisposed { get; private set; }

		#endregion

		#region Public Methods

		#region Count

		public int Count() => _context.Set<E>().Count();

		public async Task<int> CountAsync() => await _context.Set<E>().CountAsync();

		#endregion

		#region Get

		public virtual M Get(params object[] keys)
		{
			E? entity = _context.Set<E>().Find(keys);
			return _context.Mapper.Map<M>(entity);
		}

		public virtual async Task<M> GetAsync(params object[] keys)
		{
			E? entity = await _context.Set<E>().FindAsync(keys);
			return _context.Mapper.Map<M>(entity);
		}

		public ICollection<M> GetAll()
		{
			IQueryable<E> queryable = _context.Set<E>();
			Debug.WriteLine(queryable.ToQueryString());
			return _context.Mapper.Map<ICollection<M>>(queryable.ToList());
		}

		public virtual async Task<ICollection<M>> GetAllAsync()
		{
			_context.Database.SetCommandTimeout(30);
			var entities = await _context.Set<E>().ToListAsync();
			return _context.Mapper.Map<ICollection<M>>(entities);
		}

		public IQueryable<M> GetAllIncluding(params Expression<Func<E, object>>[] includeProperties)
		{
			IQueryable<E> queryable = _context.Set<E>();
			foreach (Expression<Func<E, object>> includeProperty in includeProperties)
			{
				queryable = queryable.Include<E, object>(includeProperty);
			}

			//return _context.Mapper.Map<IQueryable<M>>(queryable);
			return queryable.ProjectTo<M>(_context.Mapper.ConfigurationProvider);
		}

		#endregion

		#region Find

		public virtual M Find(Expression<Func<E, bool>> match)
		{
			var query = _context.Set<E>().Where(match);
			Debug.WriteLine(query.ToQueryString());
			var entity = query.SingleOrDefault();
			return _context.Mapper.Map<M>(entity);
		}

		public virtual async Task<M> FindAsync(Expression<Func<E, bool>> match)
		{
			var query = _context.Set<E>().Where(match);
			Debug.WriteLine(query.ToQueryString());
			var entity = await query.SingleOrDefaultAsync();
			return _context.Mapper.Map<M>(entity);
		}

		public virtual ICollection<M> FindBy(Expression<Func<E, bool>> predicate)
		{
			var query = _context.Set<E>().Where(predicate);
			Debug.WriteLine(query.ToQueryString());
			return _context.Mapper.Map<ICollection<M>>(query.ToList());
		}

		public virtual async Task<ICollection<M>> FindByAsync(Expression<Func<E, bool>> predicate)
		{
			var query = _context.Set<E>().Where(predicate);
			Debug.WriteLine(query.ToQueryString());
			var entities = await query.ToListAsync();
			return _context.Mapper.Map<ICollection<M>>(entities);
		}

		public ICollection<M> FindAll(Expression<Func<E, bool>> match)
		{
			var query = _context.Set<E>().Where(match);
			Debug.WriteLine(query.ToQueryString());
			var entities = query.ToList();
			return _context.Mapper.Map<ICollection<M>>(entities);
		}

		public async Task<ICollection<M>> FindAllAsync(Expression<Func<E, bool>> match)
		{
			var query = _context.Set<E>().Where(match);
			Debug.WriteLine(query.ToQueryString());
			var entities = await query.ToListAsync();
			return _context.Mapper.Map<ICollection<M>>(entities);
		}

		#endregion

		#endregion 

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
