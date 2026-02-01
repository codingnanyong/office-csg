using Microsoft.AspNetCore.Mvc.ApiExplorer;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using System.Reflection;

namespace CSG.MI.FDW.OpenApi.Infrastructure
{
	/// <summary>
	/// Implement the IConfigureNamedOptions interface to configure SwaggerGenOptions
	/// </summary>
	public class ConfigureSwaggerOptions : IConfigureNamedOptions<SwaggerGenOptions>
	{
		private readonly IApiVersionDescriptionProvider _provider;

        #region Public Methods

        /// <summary>
        /// 
        /// </summary>
        /// <param name="provider"></param>
        public ConfigureSwaggerOptions(IApiVersionDescriptionProvider provider)
		{
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        }

		/// <summary>
		/// Configure each API discovered for Swagger Documentation
		/// </summary>
		/// <param name="options"></param>
		public void Configure(SwaggerGenOptions options)
		{
            ConfigureSwaggerDocument(options, "1.5");
            ConfigureSwaggerDocument(options, "1.6");
        }

        /// <summary>
        /// Configure Swagger Options. Inherited from the Interface
        /// </summary>
        /// <param name="name"></param>
        /// <param name="options"></param>
        public void Configure(string? name, SwaggerGenOptions options)
		{
			Configure(options);
		}

        #endregion

        #region Private Methods

        /// <summary>
        /// Create information about the version of the API
        /// </summary>
        /// <param name="desc"></param>
        /// <returns>Information about the API</returns>
        private OpenApiInfo CreateVersionInfo(ApiVersionDescription desc)
		{
			var info = new OpenApiInfo()
			{
				Title = "FDW.OpenApi",
				Version = desc.ApiVersion.ToString(),
				Description = "<b>Description</b><br>" +
							  "This is an API written in ASP.NET Core to utilize data from <b>FDW(Factory Data Warehouse).</b><br>" +
							  "Download JSON, CSV and XML files using <b>URL</b>. ex)~/file?format=json",
				Contact = new OpenApiContact
				{
					Name = "Development Team",
					Email = "dev-team@example.com",
					Url = new Uri("https://github.com/your-org/fdw_api")
				}
			};

			if (desc.IsDeprecated)
			{
				info.Description += " This API version has been deprecated. Please use one of the new APIs available from the explorer.";
			}

			return info;
		}

        /// <summary>
        /// Swagger Document Option
        /// </summary>
        /// <param name="options">Option</param>
        /// <param name="apiVersion">Version</param>
        private void ConfigureSwaggerDocument(SwaggerGenOptions options, string apiVersion)
        {
            foreach (var description in _provider.ApiVersionDescriptions)
            {
                if (description.ApiVersion.ToString() == apiVersion)
                {
                    options.SwaggerDoc(description.GroupName, CreateVersionInfo(description));

                    var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
                    var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
                    options.IncludeXmlComments(xmlPath, true);
                }
            }
        }

        #endregion
    }
}
