using Blazor.Analytics;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Web;
using CSG.MI.TrMontrgSrv.Web.Areas.Identity;
using CSG.MI.TrMontrgSrv.Web.Data;
using CSG.MI.TrMontrgSrv.Web.Infrastructure;
using CSG.MI.TrMontrgSrv.Web.Services;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Components.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;


var builder = WebApplication.CreateBuilder(args);

NLogConfig.Setup("TrMontrgSrv.Web");

// Initialize application settings
AppSettingsProvider.DbConnectionString = builder.Configuration["ConnectionStrings:DefaultConnection"];
AppSettingsProvider.WebApiHostUri = builder.Configuration["WebApi:HostUri"];
AppSettingsProvider.WebApiVersion = Convert.ToInt32(builder.Configuration["WebApi:Version"]);
AppSettingsProvider.WebsiteHostUri = builder.Configuration["Website:HostUri"];
AppSettingsProvider.ServiceActivateMontrgSvc = Convert.ToBoolean(builder.Configuration["Service:ActivateMontrgSvc"]);
AppSettingsProvider.GoogleAnalyticsMeasurementId = builder.Configuration["GoogleAnalytics:MeasurementId"];
AppSettingsProvider.EmailAccountSmtpHost = builder.Configuration["EmailAccount:SmtpHost"];
AppSettingsProvider.EmailAccountSmtpPort = Convert.ToInt32(builder.Configuration["EmailAccount:SmtpPort"]);
AppSettingsProvider.EmailAccountId = builder.Configuration["EmailAccount:Id"];
AppSettingsProvider.EmailAccountPassword = builder.Configuration["EmailAccount:Password"];


var connectionString = builder.Configuration["ConnectionStrings:DefaultConnection"];

builder.Services.ConfigureLoggerService(); // Logger service

builder.Services.AddDbContext<ApplicationDbContext>(options => options.UseSqlServer(connectionString));
builder.Services.AddDefaultIdentity<IdentityUser>(options => options.SignIn.RequireConfirmedAccount = true)
    .AddEntityFrameworkStores<ApplicationDbContext>();

builder.Services.AddRazorPages();
builder.Services.AddServerSideBlazor();
builder.Services.AddScoped<AuthenticationStateProvider, RevalidatingIdentityAuthenticationStateProvider<IdentityUser>>();
builder.Services.AddDatabaseDeveloperPageExceptionFilter();
// Google Analytics
builder.Services.AddGoogleAnalytics(AppSettingsProvider.GoogleAnalyticsMeasurementId);

//builder.Services.AddFluentEmail("{FROM_EMAIL}", "AutoEmailer")
//        .AddRazorRenderer()
//        .AddSmtpSender("dsnet2.dskorea.com", 25);
//builder.Services.AddScoped<IEmailSender, EmailSender>();

//builder.Services.AddHttpClient<ITrDataService, TrDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IDeviceDataService, DeviceDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IFrameDataService, FrameDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<ISnapDataService, SnapDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<ICfgDataService, CfgDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IImrDataService, ImrDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));

builder.Services.AddHttpClient<IDeviceCtrlDataService, DeviceCtrlDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IFrameCtrlDataService, FrameCtrlDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IRoiCtrlDataService, RoiCtrlDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IBoxCtrlDataService, BoxCtrlDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IEvntLogDataService, EvntLogDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IMailingAddrDataService, MailingAddrDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));
builder.Services.AddHttpClient<IGrpKeyDataService, GrpKeyDataService>(client => client.BaseAddress = new Uri(AppSettingsProvider.WebApiHostUri));

builder.Services.AddScoped<IMontrgService, MontrgService>();
builder.Services.AddScoped<IAppUiCfgProvider, AppUiCfgProvider>();


var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
    app.UseMigrationsEndPoint();
}
else
{
    app.UseExceptionHandler("/ErrorPg");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

//app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();

app.UseAuthentication();
app.UseAuthorization();

app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
    endpoints.MapBlazorHub();
    endpoints.MapFallbackToPage("/_Host");
});

using (var serviceScope = app.Services.CreateScope())
{
    var services = serviceScope.ServiceProvider;

    // Monitoring service
    if (AppSettingsProvider.ServiceActivateMontrgSvc)
    {
        var montrgSvc = services.GetRequiredService<IMontrgService>();
        montrgSvc.Start();
    }
}

app.Run();