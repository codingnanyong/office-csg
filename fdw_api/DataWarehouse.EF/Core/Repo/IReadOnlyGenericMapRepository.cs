using System.Linq.Expressions;

namespace CSG.MI.FDW.EF.Core.Repo
{
	public interface IReadOnlyGenericMapRepository<E, M> where E : class
														 where M : class
	{
		#region Count

		int Count();

		Task<int> CountAsync();

		#endregion

		#region Get

		M Get(params object[] keys);

		ICollection<M> GetAll();

		Task<ICollection<M>> GetAllAsync();

		IQueryable<M> GetAllIncluding(params Expression<Func<E, object>>[] includeProperties);

		Task<M> GetAsync(params object[] keys);

		#endregion

		#region Find

		M Find(Expression<Func<E, bool>> match);

		ICollection<M> FindAll(Expression<Func<E, bool>> match);

		Task<ICollection<M>> FindAllAsync(Expression<Func<E, bool>> match);

		Task<M> FindAsync(Expression<Func<E, bool>> match);

		ICollection<M> FindBy(Expression<Func<E, bool>> predicate);

		Task<ICollection<M>> FindByAsync(Expression<Func<E, bool>> predicate);

		#endregion

		void Dispose();
	}
}
