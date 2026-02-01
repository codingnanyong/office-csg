using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.TrDataImporterSvc.Core;
using CSG.MI.TrMontrgSrv.TrDataImporterSvc.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Workers
{
    public class ZipFileMonitoringWorker : BackgroundService
    {
        #region Fields

        private readonly ILogger<ZipFileMonitoringWorker> _svclogger;
        private readonly CommandLineOptions _options;

        private readonly ILoggerManager _logger;
        private List<Folder> _folders = new();

        #endregion

        #region Constructors

        public ZipFileMonitoringWorker(ILogger<ZipFileMonitoringWorker> svclogger,
                                       ILoggerManager logger,
                                       CommandLineOptions options)
        {
            _svclogger = svclogger;
            _logger = logger;
            _options = options;
        }

        #endregion

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _svclogger.LogInformation("Service started");

            List<Folder> folders = new();
            bool hasError = false;

            do
            {
                try
                {
                    _folders = FolderManager.LoadWatchingFolders();

                    string dirs = String.Empty;
                    _folders.ForEach(x => dirs += "+ " + x.FullPath + Environment.NewLine);
                    dirs = dirs.Trim(Environment.NewLine.ToCharArray());

                    _logger.LogInfo($"Found {_folders.Count} directories to watch.{Environment.NewLine}{dirs}");

                    // Start monitoring
                    _folders.ForEach(x => x.Start());

                    hasError = false;
                }
                catch (Exception ex)
                {
                    hasError = true;

                    _logger.LogError(ex.Message);
                    _svclogger.LogError(ex.Message);

                    // 1 min 대기 후 재시작
                    await Task.Delay((int)TimeSpan.FromSeconds(60).TotalMilliseconds, stoppingToken);

                    string msg = "Trying to restart the folder monitoring...";
                    _logger.LogInfo(msg);
                    _svclogger.LogInformation(msg);
                }
            } while (hasError);

            var tcs = new TaskCompletionSource<bool>();
            stoppingToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            await tcs.Task;

            try
            {
                _folders.ForEach(x => x.Stop());

                Thread.Sleep(2000);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
            }

            _svclogger.LogInformation("Service stopped");

        }


    }
}
