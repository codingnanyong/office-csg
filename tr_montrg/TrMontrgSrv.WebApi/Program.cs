using CSG.MI.TrMontrgSrv.BLL;
using CSG.MI.TrMontrgSrv.BLL.AutoBatch;
using CSG.MI.TrMontrgSrv.BLL.AutoBatch.Interface;
using CSG.MI.TrMontrgSrv.BLL.Dashboard;
using CSG.MI.TrMontrgSrv.BLL.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.EF.Repositories.Inspection;
using CSG.MI.TrMontrgSrv.EF.Repositories.Inspection.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.Dashboard;
using CSG.MI.TrMontrgSrv.EF.Repositories.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.WebApi;
using CSG.MI.TrMontrgSrv.WebApi.Core.Extentions;
using CSG.MI.TrMontrgSrv.WebApi.Infrastructure;
using CSG.MI.TrMontrgSrv.WebApi.Infrastructure.Binders;
using CSG.MI.TrMontrgSrv.WebApi.Infrastructure.Swagger;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Swashbuckle.AspNetCore.SwaggerGen;
using System.Text.Json.Serialization;
using System.Linq;

const string ALLOW_SPECIFI_CORIGINS = "_allowSpecificOrigins";

var builder = WebApplication.CreateBuilder(args);

NLogConfig.Setup("TrMontrgSrv.WebApi");


builder.Services.ConfigureCors(ALLOW_SPECIFI_CORIGINS);
builder.Services.ConfigureLoggerService(); // Logger service
builder.Services.AddControllers(options =>
{
    options.ModelBinderProviders.Insert(0, new QueryBooleanModelBinderProvider());
    options.RespectBrowserAcceptHeader = true;
    options.ReturnHttpNotAcceptable = true;
});
//}).AddXmlDataContractSerializerFormatters();

builder.Services.AddSwaggerGen();
//builder.Services.AddSwaggerGen(c =>
//{
//    c.SwaggerDoc("v1", new OpenApiInfo { Title = "CSG.MI.TrMontrgSrv.WebApi", Version = "v1" });
//});

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration["Data:TrMontrgSrv:ConnectionString"])
);
builder.Services.Configure<JsonOptions>(options =>
{
    options.JsonSerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
});
builder.Services.Configure<ApiBehaviorOptions>(options =>
{
    options.SuppressModelStateInvalidFilter = true;
});

builder.Services.AddTransient<IDeviceRepository, DeviceRepository>();
builder.Services.AddTransient<IFrameRepository, FrameRepository>();
builder.Services.AddTransient<IRoiRepository, RoiRepository>();
builder.Services.AddTransient<IBoxRepository, BoxRepository>();
builder.Services.AddTransient<ICfgRepository, CfgRepository>();
builder.Services.AddTransient<IMediumRepository, MediumRepository>();

builder.Services.AddTransient<IDeviceRepo, DeviceRepo>();
builder.Services.AddTransient<IFrameRepo, FrameRepo>();
builder.Services.AddTransient<IRoiRepo, RoiRepo>();
builder.Services.AddTransient<IBoxRepo, BoxRepo>();
builder.Services.AddTransient<ICfgRepo, CfgRepo>();
builder.Services.AddTransient<IMediumRepo, MediumRepo>();

builder.Services.AddTransient<IDeviceCtrlRepository, DeviceCtrlRepository>();
builder.Services.AddTransient<IFrameCtrlRepository, FrameCtrlRepository>();
builder.Services.AddTransient<IRoiCtrlRepository, RoiCtrlRepository>();
builder.Services.AddTransient<IBoxCtrlRepository, BoxCtrlRepository>();
builder.Services.AddTransient<IEvntLogRepository, EvntLogRepository>();
builder.Services.AddTransient<IMailingAddrRepository, MailingAddrRepository>();
builder.Services.AddTransient<IGrpKeyRepository, GrpKeyRepository>();

builder.Services.AddTransient<IDeviceCtrlRepo, DeviceCtrlRepo>();
builder.Services.AddTransient<IFrameCtrlRepo, FrameCtrlRepo>();
builder.Services.AddTransient<IRoiCtrlRepo, RoiCtrlRepo>();
builder.Services.AddTransient<IBoxCtrlRepo, BoxCtrlRepo>();
builder.Services.AddTransient<IEvntLogRepo, EvntLogRepo>();
builder.Services.AddTransient<IMailingAddrRepo, MailingAddrRepo>();
builder.Services.AddTransient<IGrpKeyRepo, GrpKeyRepo>();

builder.Services.AddTransient<IFrameImrCtrlRepo, FrameCtrlRepo>();
builder.Services.AddTransient<IRoiImrCtrlRepo, RoiCtrlRepo>();

#region Dashboard Services

builder.Services.AddTransient<IDashboardRepository, DashboardRepository>();
builder.Services.AddTransient<IDashboardRepo, DashboardRepo>();

#endregion

#region Inspection Services

builder.Services.AddTransient<IInspectionRepository, InspectionRepository>();
builder.Services.AddTransient<IInspectionRepo, InspectionRepo>();

#endregion

builder.Services.AddRouting(options => options.LowercaseUrls = true);
builder.Services.AddVersioning();
builder.Services.AddTransient<IConfigureOptions<SwaggerGenOptions>, ConfigureSwaggerOptions>();

var app = builder.Build();

/*if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}
*/

var apiVersionDescriptionProvider = app.Services.GetRequiredService<IApiVersionDescriptionProvider>();

app.UseCors(ALLOW_SPECIFI_CORIGINS);
app.UseSwagger();
app.UseSwaggerUI(options =>
{
    foreach (var description in apiVersionDescriptionProvider.ApiVersionDescriptions.OrderByDescending(d => d.ApiVersion))
    {
        options.SwaggerEndpoint($"/swagger/{description.GroupName}/swagger.json", description.GroupName.ToLowerInvariant());
    }
    options.DefaultModelsExpandDepth(-1);
    options.DocExpansion(Swashbuckle.AspNetCore.SwaggerUI.DocExpansion.None);
});
var serviceScope = app.Services.GetService<IServiceScopeFactory>().CreateScope();
var services = serviceScope.ServiceProvider; 
var logger = services.GetRequiredService<ILoggerManager>();

app.ConfigureExceptionHandler(logger);

/*using (var serviceScope = app.Services.GetService<IServiceScopeFactory>().CreateScope())
{
    var context = serviceScope.ServiceProvider.GetRequiredService<AppDbContext>();

    var services = serviceScope.ServiceProvider;

    if (app.Environment.IsDevelopment())
    {
        var provider = services.GetRequiredService<IApiVersionDescriptionProvider>();

        //app.UseSwagger(options =>
        //{
        //    options.RouteTemplate = "api-docs/{documentName}/docs.json";
        //});
        app.UseSwagger();
        app.UseSwaggerUI(options =>
        {
            // https://www.telerik.com/blogs/your-guide-rest-api-versioning-aspnet-core
            foreach (var description in provider.ApiVersionDescriptions)
            {
                options.SwaggerEndpoint($"/swagger/{description.GroupName}/swagger.json",
                                        description.GroupName.ToUpperInvariant());
            }
        });
    }

    var logger = services.GetRequiredService<ILoggerManager>();
    app.ConfigureExceptionHandler(logger);
}*/


//app.UseHttpsRedirection();
app.UseForwardedHeaders(new ForwardedHeadersOptions { ForwardedHeaders = ForwardedHeaders.All });

app.UseRouting();
app.UseAuthorization();
app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
});

app.Run();