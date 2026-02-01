using CSG.MI.FDW.LoggerService;
using Microsoft.AspNetCore.Diagnostics;
using System.Net;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Core.Extentions
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
