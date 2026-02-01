namespace CSG.MI.FDW.OpenApi.Infrastructure.Formatter
{
	/// <summary>
	/// Implement generic data list CSV download Service Interface
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public interface ICsvResultService<T>
	{
		/// <summary>
		/// Generic data list CSV download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		MemoryStream Download(ICollection<T> data, string fileName);

		/// <summary>
		/// Generic data CSV download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		MemoryStream Download(T data, string fileName);
	}
}
