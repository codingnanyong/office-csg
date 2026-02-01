using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace CSG.MI.TrMontrgSrv.WebApi.Core.Extentions
{
    public static class ServiceExtensions
    {
        public static void ConfigureCors(this IServiceCollection services, string name) =>
            services.AddCors(options =>
            {
                options.AddPolicy(name,
                                  policy =>
                                  {
                                      policy //.WithOrigins("http://localhost:9000", "https://localhost:44355", "http://localhost:37508")
                                            .AllowAnyOrigin()
                                            .AllowAnyHeader()
                                            .AllowAnyMethod();
                                  });
            });

        public static void ConfigureIISIntegration(this IServiceCollection services) =>
            services.Configure<IISOptions>(options =>
            {

            });

    }
}
