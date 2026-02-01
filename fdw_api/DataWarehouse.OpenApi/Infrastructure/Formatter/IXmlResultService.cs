namespace CSG.MI.FDW.OpenApi.Infrastructure.Formatter
{
	/// <summary>
	/// Implement generic data list XML download Service Interface
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public interface IXmlResultService<T>
	{
		/// <summary>
		/// Generic data list XML download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		MemoryStream Download(List<T> data, string fileName);

		/// <summary>
		/// Generic data XML download method
		/// </summary>
		/// <param name="data"></param>
		/// <param name="fileName"></param>
		/// <returns></returns>
		MemoryStream Download(T data, string fileName);
	}
}
