using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using CommandLine;
using CSG.MI.TrMontrgSrv.BLL;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Helpers;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.TrDataImporterSvc.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.EventLog;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc
{
    class Program
    {
        private static ILoggerManager _logger;

        static async Task<int> Main(string[] args)
        {
#if DEBUG
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Debug");
#endif

            NLogConfig.Setup(App.AppName);
            _logger = new LoggerManager();
            _logger.LogInfo($"===== Launched {App.AppName} ==============================");
            _logger.LogInfo($"{App.AppName}.exe " + String.Join(" ", args));
            _logger.LogInfo($"WorkingDirectory: {App.WorkingPath}");

            Console.Title = App.AppName;

            if (ProcessHelper.TotalOtherSameProcesses() >= 1)
            {
                _logger.LogWarn($"{App.AppName} is already running.");
                return await Task.FromResult(-1);
            }

            return await Parser.Default.ParseArguments<CommandLineOptions>(args)
                .MapResult(async (CommandLineOptions opts) =>
                {
                    await CreateHostBuilder(args, opts).Build().RunAsync();
                    return 0;
                },
                errs => Task.FromResult(-1));
        }

        public static IHostBuilder CreateHostBuilder(string[] args, CommandLineOptions opts) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureLogging(configureLogging =>
                    configureLogging.AddFilter<EventLogLoggerProvider>(level => level >= LogLevel.Information))
                .ConfigureAppConfiguration((context, builder) => {
                    builder.SetBasePath(App.WorkingPath)
                           .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                           .AddJsonFile($"appsettings.{context.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: true)
                           .AddEnvironmentVariables();
                })
                .ConfigureServices((hostContext, services) =>
                {
                    services.ConfigureLoggerService();
                    services.AddSingleton(opts);
                    services.AddHostedService<ZipFileMonitoringWorker>()
                        .Configure<EventLogSettings>(config =>
                        {
                            config.LogName = App.ServiceName;
                            config.SourceName = App.ServiceSourceName;
                        });
                    services.AddTransient<IDeviceRepository, DeviceRepository>();
                    services.AddTransient<IDeviceRepo, DeviceRepo>();
                    services.AddTransient<IFrameRepo, FrameRepo>();
                    services.AddTransient<IRoiRepo, RoiRepo>();
                    services.AddTransient<IBoxRepo, BoxRepo>();
                    services.AddTransient<ICfgRepo, CfgRepo>();
                    services.AddTransient<IMediumRepo, MediumRepo>();
                }).UseWindowsService();
    }
}
