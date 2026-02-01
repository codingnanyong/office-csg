namespace CSG.MI.FDW.OpenApi.Infrastructure.Formatter
{
	/// <summary>
	/// Implement generic data list JSON download Service Interface
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public interface IJsonResultService<T>
	{
		/// <summary>
		/// Generic data list JSON download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		MemoryStream Download(ICollection<T> data, string fileName);

		/// <summary>
		/// Generic data JSON download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		MemoryStream Download(T data, string fileName);
	}
}
