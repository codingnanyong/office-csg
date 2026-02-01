using CSG.MI.FDW.BLL.Analysis;
using CSG.MI.FDW.BLL.Analysis.Interface;
using CSG.MI.FDW.BLL.Feedbacks;
using CSG.MI.FDW.BLL.Feedbacks.Interface;
using CSG.MI.FDW.BLL.IoT;
using CSG.MI.FDW.BLL.IoT.Interface;
using CSG.MI.FDW.BLL.Production.DataMart;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.BLL.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.BLL.Production.RTLS;
using CSG.MI.FDW.BLL.Production.RTLS.Interface;
using CSG.MI.FDW.EF.Repositories.Feedbacks;
using CSG.MI.FDW.EF.Repositories.Feedbacks.Interface;
using CSG.MI.FDW.EF.Repositories.IoT;
using CSG.MI.FDW.EF.Repositories.IoT.Interface;
using CSG.MI.FDW.EF.Repositories.Production.DataMart;
using CSG.MI.FDW.EF.Repositories.Production.DataMart.Interface;
using CSG.MI.FDW.EF.Repositories.Production.PCC;
using CSG.MI.FDW.EF.Repositories.Production.PCC.Interface;
using CSG.MI.FDW.EF.Repositories.Production.RTLS;
using CSG.MI.FDW.EF.Repositories.Production.RTLS.Intreface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Data;
using Microsoft.AspNetCore.Mvc.ApiExplorer;
using Microsoft.EntityFrameworkCore;
using NLog;
using NLog.Web;
using Repositories.Analysis;
using Repositories.Analysis.Interface;

var logger = NLog.LogManager.Setup().LoadConfigurationFromAppSettings().GetCurrentClassLogger();

try
{
	var builder = WebApplication.CreateBuilder(args);

	builder.Logging.ClearProviders();
	builder.Host.UseNLog();

	// Add services to the container.
	builder.Services.AddControllers();

	builder.Services.AddDbContext<HQDbContext>(options => options.UseNpgsql(builder.Configuration["FDW:HQ:ConnectionString"]));
    builder.Services.AddDbContext<IoTDbContext>(options => options.UseNpgsql(builder.Configuration["FDW:IoT:ConnectionString"]));

    // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
    builder.Services.AddEndpointsApiExplorer();
	builder.Services.AddSwaggerGen();

	//Service Repository,Repo Add
	#region RTLS Data Services

	builder.Services.AddTransient<IProcessMstRepository, ProcessMstRepository>();
	builder.Services.AddTransient<IProcessMstRepo, ProcessMstRepo>();
	builder.Services.AddTransient<IEslLocationRepository, EslLocationRepository>();
	builder.Services.AddTransient<IEslLocationRepo, EslLocationRepo>();
	builder.Services.AddTransient<IErpPlanRepository, ErpPlanRepository>();
	builder.Services.AddTransient<IErpPlanRepo, ErpPlanRepo>();

	#endregion

	#region PCC Data Services

	builder.Services.AddTransient<IOpMstRepository, OpMstRepository>();
	builder.Services.AddTransient<IOpMstRepo, OpMstRepo>();
	builder.Services.AddTransient<IIssueMstRepository, IssueMstRepository>();
	builder.Services.AddTransient<IIssueMstRepo, IssueMstRepo>();
	builder.Services.AddTransient<IMachineMstRepository, MachineMstRepository>();
	builder.Services.AddTransient<IMachineMstRepo, MachineMstRepo>();

    #endregion

    #region IoT Data Services

    builder.Services.AddTransient<IEnviromentRepository, EnviromentRepository>();
    builder.Services.AddTransient<IEnviromentRepo, EnviromentRepo>();

    #endregion

    #region Production Data Services

    builder.Services.AddTransient<IDailyStatusRepository, DailyStatusRepository>();
	builder.Services.AddTransient<IDailyStatusRepo, DailyStatusRepo>();
	builder.Services.AddTransient<IWsSampleRepository, WsSampleRepository>();
	builder.Services.AddTransient<IWsSampleRepo, WsSampleRepo>();
	builder.Services.AddTransient<IWsWipRepository, WsWipRepository>();
	builder.Services.AddTransient<IWsWipRepo, WsWipRepo>();
    builder.Services.AddTransient<IWsCoordinateRepository, WsCoordinateRepository>();
    builder.Services.AddTransient<IWsCoordinateRepo, WsCoordinateRepo>();
	builder.Services.AddTransient<IWsWipRateRepository, WsWipRateRepository>();
	builder.Services.AddTransient<IWsWipRateRepo, WsWipRateRepo>();

    builder.Services.AddTransient<IFactoryPlnTotalRepository, FactoryPlnTotalRepository>();
	builder.Services.AddTransient<IFactoryPlnTotalRepo, FactoryPlnTotalRepo>();
	builder.Services.AddTransient<IFactoryPrfTotalRepository, FactoryPrfTotalRepository>();
	builder.Services.AddTransient<IFactoryPrfTotalRepo, FactoryPrfTotalRepo>();

    #endregion

    #region Analysis Data Services

    builder.Services.AddTransient<IDevStylePredictionRepository, DevStylePredictionRepository>();
    builder.Services.AddTransient<IDevStylePredictionRepo, DevStylePredictionRepo>();

    #endregion

    #region Feedbakc Services

    builder.Services.AddTransient<IFeedbackRepository, FeedbackRepository>();
	builder.Services.AddTransient<IFeedbackRepo, FeedbackRepo>();
    builder.Services.AddTransient<ICategoryRepository, CategoryRepository>();
    builder.Services.AddTransient<ICategoryRepo, CategoryRepo>();

    #endregion

    #region Document Output (JSON,CSV,XML)

    builder.Services.AddScoped(typeof(IJsonResultService<>), typeof(JsonResultService<>));
	builder.Services.AddScoped(typeof(ICsvResultService<>), typeof(CsvResultService<>));
	builder.Services.AddScoped(typeof(IXmlResultService<>), typeof(XmlResultService<>));

	#endregion

	builder.Services.ConfigureLoggerService();
	builder.Services.AddRouting(options => options.LowercaseUrls = true);
	builder.Services.AddVersioning();
	builder.Services.ConfigureOptions<ConfigureSwaggerOptions>();

    var allowedOrigin = builder.Configuration.GetSection("AllowedOrigins").Get<string[]>();

    builder.Services.AddCors(options =>
	{
		options.AddPolicy("AllowAnyOrigin", builder =>
		{
			builder.WithOrigins(allowedOrigin!)
				   .AllowAnyHeader()
				   .AllowAnyMethod();
		});
	});

	var app = builder.Build();

	var apiVersionDescriptionProvider = app.Services.GetRequiredService<IApiVersionDescriptionProvider>();

	app.UseCors("AllowAnyOrigin");

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

    app.UseMiddleware<NLog.Web.NLogRequestPostedBodyMiddleware>();

	app.UseHttpsRedirection();

	app.UseAuthorization();

	app.MapControllers();

	app.Run();
}
catch (Exception ex)
{
	logger.Error(ex);
	throw;
}
finally
{
	NLog.LogManager.Shutdown();
}