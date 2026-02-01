using CSG.MI.TrMontrgSrv.Dashboard.Components;
using CSG.MI.TrMontrgSrv.Dashboard.Infrastructure;
using CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard;
using CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard.Interface;
using System.Globalization;

var builder = WebApplication.CreateBuilder(args);

// Language Settings
builder.Services.AddLocalization();
builder.Services.AddControllers();

// Initialize application settings
AppSettingProvider.DbConnectionString = builder.Configuration["ConnectionStrings:DefaultConnection"] ?? default!;
AppSettingProvider.WebApiHostUri = builder.Configuration["WebApi:HostUri"] ?? default!;
AppSettingProvider.WebApiVersion = Convert.ToInt32(builder.Configuration["WebApi:Version"]);
AppSettingProvider.FDWApiHostUri = builder.Configuration["FdwApi:HostUri"] ?? default!;
AppSettingProvider.FDWApiVersion = Convert.ToSingle(builder.Configuration["FdwApi:Version"]);

builder.Services.AddHttpClient<IDashboardDataService, DashboardDataService>(client => client.BaseAddress = new Uri(AppSettingProvider.WebApiHostUri));
builder.Services.AddHttpClient<IFdwDataService, FdwDataService>(client => client.BaseAddress = new Uri(AppSettingProvider.FDWApiHostUri));

// Add services to the Blazor Bootstrap.
builder.Services.AddBlazorBootstrap();

// Inject Services
builder.Services.AddScoped<CommonHelper>();

// Add services to the container.
builder.Services.AddRazorComponents()
                .AddInteractiveServerComponents();

var supportedCultures = new List<CultureInfo>
{
    new CultureInfo("en-US"),
    new CultureInfo("ko-KR")
};

builder.Services.Configure<RequestLocalizationOptions>(options =>
{
    options.DefaultRequestCulture =
    new Microsoft.AspNetCore.Localization.RequestCulture("en-US");
    options.SupportedUICultures = supportedCultures;
});

var app = builder.Build();

app.UseRequestLocalization();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();

app.UseStaticFiles();
app.UseAntiforgery();

app.MapControllers();

app.MapRazorComponents<App>()
   .AddInteractiveServerRenderMode();

app.Run();