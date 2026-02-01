using System.Linq.Expressions;
using Data;

namespace CSG.MI.FDW.EF.Core.Repo
{
    public interface IReadOnlyGenericRepository<T> where T : class
	{
		#region Properties

		HQDbContext Context { get; }

		#endregion

		#region Methods

		#region Count

		int Count();

		Task<int> CountAsync();

		#endregion

		#region Get

		T Get(params object[] keys);

		IQueryable<T> GetAll();

		Task<ICollection<T>> GetAllAsync();

		IQueryable<T> GetAllIncluding(params Expression<Func<T, object>>[] includeProperties);

		Task<T> GetAsync(params object[] keys);

		#endregion

		#region Find

		T Find(Expression<Func<T, bool>> match);

		ICollection<T> FindAll(Expression<Func<T, bool>> match);

		Task<ICollection<T>> FindAllAsync(Expression<Func<T, bool>> match);

		Task<T> FindAsync(Expression<Func<T, bool>> match);

		IQueryable<T> FindBy(Expression<Func<T, bool>> predicate);

		Task<ICollection<T>> FindByAsync(Expression<Func<T, bool>> predicate);

		#endregion

		#endregion // Methods

		#region IDisposable Member

		void Dispose();

		#endregion
	}
}
