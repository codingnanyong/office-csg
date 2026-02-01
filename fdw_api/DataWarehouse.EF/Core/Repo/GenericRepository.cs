using Data;

namespace CSG.MI.FDW.EF.Core.Repo
{
    // https://www.c-sharpcorner.com/article/net-entity-framework-core-generic-async-operations-with-unit-of-work-generic-re/
    public class GenericRepository<T> : ReadOnlyGenericRepository<T>, IGenericRepository<T> where T : class
	{
		#region Fields


		#endregion

		#region Constructors

		public GenericRepository(HQDbContext ctx) : base(ctx)
		{
		}

		#endregion

		#region Add/Delete/Update

		public virtual T Add(T entity)
		{
			_context.Set<T>().Add(entity);
			int affected = _context.SaveChanges();

			return affected > 0 ? entity! : null!;
		}

		public virtual async Task<T> AddAsync(T entity)
		{
			_context.Set<T>().Add(entity);
			int affected = await _context.SaveChangesAsync();

			return affected > 0 ? entity! : null!;
		}

		public virtual int AddRange(IEnumerable<T> entities)
		{
			_context.Set<T>().AddRange(entities);
			return _context.SaveChanges();
		}

		public virtual async Task<int> AddRangeAsync(IEnumerable<T> entities)
		{
			_context.Set<T>().AddRange(entities);
			return await _context.SaveChangesAsync();
		}

		public virtual int Delete(T entity)
		{
			_context.Set<T>().Remove(entity);
			return _context.SaveChanges();
		}

		public virtual async Task<int> DeleteAsync(T entity)
		{
			_context.Set<T>().Remove(entity);
			return await _context.SaveChangesAsync();
		}

		public virtual int Delete(object key, params object[] morekeys)
		{
			if (key == null)
				return 0;

			List<object> keys = new();
			keys.Add(key);

			if (morekeys != null && morekeys.Length > 0)
			{
				foreach (var k in morekeys)
					keys.Add(k);
			}

			T exist = _context.Set<T>().Find(keys.ToArray())!;
			if (exist == null)
				return 0;

			_context.Set<T>().Remove(exist);

			return _context.SaveChanges();
		}

		public virtual async Task<int> DeleteAsync(object key, params object[] morekeys)
		{
			if (key == null)
				return 0;

			List<object> keys = new();
			keys.Add(key);

			if (morekeys != null && morekeys.Length > 0)
			{
				foreach (var k in morekeys)
					keys.Add(k);
			}

			T exist = _context.Set<T>().Find(keys.ToArray())!;
			if (exist == null)
				return 0;

			_context.Set<T>().Remove(exist);

			return await _context.SaveChangesAsync();
		}

		public virtual T Update(T entity, params object[] keys)
		{
			if (entity == null)
				return null!;

			int affected = 0;
			T exist = _context.Set<T>().Find(keys)!;
			if (exist != null)
			{
				_context.Entry(exist).CurrentValues.SetValues(entity);
				affected = _context.SaveChanges();
			}

			return affected > 0 ? exist! : null!;
		}

		public virtual async Task<T> UpdateAsync(T entity, params object[] keys)
		{
			if (entity == null)
				return null!;

			int affected = 0;
			T? exist = await _context.Set<T>().FindAsync(keys);
			if (exist != null)
			{
				_context.Entry(exist).CurrentValues.SetValues(entity);
				affected = await _context.SaveChangesAsync();
			}

			return affected > 0 ? exist! : null!;
		}

		#endregion

		#region Save

		public virtual int Save()
		{
			return _context.SaveChanges();
		}

		public virtual async Task<int> SaveAsync()
		{
			return await _context.SaveChangesAsync();
		}

		#endregion

		#region Disposable

		protected override void Dispose(bool disposing)
		{
			if (IsDisposed == false)
			{
				if (disposing)
				{
					// Dispose managed objects.
					//_context?.Dispose();
				}

				// Free unmanaged resources and override a finalizer below.
				// Set large fields to null.
			}

			base.Dispose(disposing);
		}

		#endregion
	}
}
