using CSG.MI.FDW.EF.Data;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Core.Repo
{
    public class GenericMapRepository<E, M, C> : ReadOnlyGenericMapRepository<E, M, C>, IGenericMapRepository<E, M, C>
        where E : class
        where M : class
        where C : BaseDbContext
    {
        #region Constructors

        public GenericMapRepository(C ctx) : base(ctx)
        {
        }

        #endregion

        #region Add/Delete/Update

        public virtual M Add(M model)
		{
			var entity = _context.Mapper.Map<E>(model);

			_context.Set<E>().Add(entity);
			int affected = _context.SaveChanges();

			return affected > 0 ? model! : null!;
		}

		public virtual async Task<M> AddAsync(M model)
		{
			var entity = _context.Mapper.Map<E>(model);

			_context.Set<E>().Add(entity);
			int affected = await _context.SaveChangesAsync();

			return affected > 0 ? model! : null!;
		}

		public virtual int AddRange(IEnumerable<M> models)
		{
			var entities = _context.Mapper.Map<IEnumerable<E>>(models);

			_context.Set<E>().AddRange(entities);
			return _context.SaveChanges();
		}

		public virtual async Task<int> AddRangeAsync(IEnumerable<M> models)
		{
			var entities = _context.Mapper.Map<IEnumerable<E>>(models);

			_context.Set<E>().AddRange(entities);
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

			E exist = _context.Set<E>().Find(keys.ToArray())!;
			if (exist == null)
				return 0;

			_context.Set<E>().Remove(exist);

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

			E exist = _context.Set<E>().Find(keys.ToArray())!;
			if (exist == null)
				return 0;

			_context.Set<E>().Remove(exist);

			return await _context.SaveChangesAsync();
		}

		public virtual M Update(M model, params object[] keys)
		{
			if (model == null)
				return null!;

			var entity = _context.Mapper.Map<E>(model);

			int affected = 0;
			E exist = _context.Set<E>().Find(keys)!;
			if (exist != null)
			{
				_context.Entry(exist).CurrentValues.SetValues(entity);
				affected = _context.SaveChanges();
			}

			return affected > 0 ? _context.Mapper.Map<M>(entity) : null!;
		}

		public virtual async Task<M> UpdateAsync(M model, params object[] keys)
		{
			if (model == null)
				return null!;

			var entity = _context.Mapper.Map<E>(model);

			int affected = 0;
			E? exist = await _context.Set<E>().FindAsync(keys);
			if (exist != null)
			{
				_context.Entry(exist).CurrentValues.SetValues(entity);
				affected = await _context.SaveChangesAsync();
			}

			return affected > 0 ? _context.Mapper.Map<M>(entity) : null!;
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

			// Call base class implementation
			base.Dispose(disposing);
		}

		#endregion
	}
}
