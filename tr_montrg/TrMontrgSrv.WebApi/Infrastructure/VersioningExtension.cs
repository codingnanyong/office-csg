using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Versioning;
using Microsoft.Extensions.DependencyInjection;

namespace CSG.MI.TrMontrgSrv.WebApi.Infrastructure
{
    public static class VersioningExtension
    {
        public static void AddVersioning(this IServiceCollection services)
        {
            services.AddApiVersioning(options =>
            {
                options.ReportApiVersions = true;
                options.DefaultApiVersion = new ApiVersion(1, 0);
                options.AssumeDefaultVersionWhenUnspecified = true;
                //options.ApiVersionReader = new HeaderApiVersionReader("api-version");

                //options.ApiVersionReader = ApiVersionReader.Combine(
                //                                new HeaderApiVersionReader("x-api-version"),    // x-api-version header value of 2
                //                                new MediaTypeApiVersionReader("v"),             // applcation/json;v=2 in Accept header,
                //                                new QueryStringApiVersionReader("version")      // /api/bands/4?version=2
                //                           );
            });
            services.AddVersionedApiExplorer(options =>
            {
                options.GroupNameFormat = "'v'VVV"; // v[major][.minor][-status]

                // note: this option is only necessary when versioning by url segment. the SubstitutionFormat
                // can also be used to control the format of the API version in route templates
                options.SubstituteApiVersionInUrl = true;
            });
        }
    }
}
