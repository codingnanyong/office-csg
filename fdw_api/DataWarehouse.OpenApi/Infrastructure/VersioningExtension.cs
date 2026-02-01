using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.FDW.OpenApi.Infrastructure
{
	/// <summary>
	/// Provides extension methods to facilitate API versioning.
	/// </summary>
	public static class VersioningExtension
	{
		/// <summary>
		/// Implement add Versioning Method
		/// </summary>
		/// <param name="services"></param>
		public static void AddVersioning(this IServiceCollection services)
		{
			services.AddApiVersioning(options =>
			{
				options.DefaultApiVersion = new ApiVersion(1, 0);
				options.AssumeDefaultVersionWhenUnspecified = true;
				options.ReportApiVersions = true;
			});

			services.AddVersionedApiExplorer(options =>
			{
				options.GroupNameFormat = "'v'VVV"; // v[major][.minor][-status]

				options.SubstituteApiVersionInUrl = true;
			});
		}
	}
}
