namespace CSG.MI.FDW.EF.Core.Repo
{
	public interface IGenericRepository<T> : IReadOnlyGenericRepository<T> where T : class
	{
		#region Add/Delete/Update

		T Add(T entity);

		Task<T> AddAsync(T entity);

		int AddRange(IEnumerable<T> entities);

		Task<int> AddRangeAsync(IEnumerable<T> entities);

		int Delete(T entity);

		Task<int> DeleteAsync(T entity);

		int Delete(object key, params object[] morekeys);

		Task<int> DeleteAsync(object key, params object[] morekeys);

		T Update(T entity, params object[] keys);

		Task<T> UpdateAsync(T entity, params object[] keys);

		#endregion

		#region Save

		int Save();

		Task<int> SaveAsync();

		#endregion
	}
}
