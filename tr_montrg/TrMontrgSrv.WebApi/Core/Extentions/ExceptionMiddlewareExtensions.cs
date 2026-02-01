using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mime;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;

namespace CSG.MI.TrMontrgSrv.WebApi.Core.Extentions
{
    public static class ExceptionMiddlewareExtensions
    {
        public static void ConfigureExceptionHandler(this IApplicationBuilder app, ILoggerManager logger)
        {
            app.UseExceptionHandler(appError =>
            {
                appError.Run(async ctx =>
                {
                    ctx.Response.StatusCode = (int)HttpStatusCode.InternalServerError;
                    ctx.Response.ContentType = MediaTypeNames.Application.Json;

                    var ctxFeature = ctx.Features.Get<IExceptionHandlerFeature>();
                    if (ctxFeature != null)
                    {
                        logger.LogError(ctxFeature.Error.ToFormattedString());

                        await ctx.Response.WriteAsync(
                            new ErrorDetail()
                            {
                                StatusCode = ctx.Response.StatusCode,
                                Message = "Internal server error"
                            }.ToString());
                    }
                });
            });
        }
    }
}
