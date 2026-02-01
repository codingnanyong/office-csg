using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using CSG.MI.TrMontrgSrv.Web.Infrastructure;
using CSG.MI.TrMontrgSrv.Web.Montrg;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using FluentEmail.Core;
using FluentEmail.Core.Models;
using FluentEmail.Smtp;

namespace CSG.MI.TrMontrgSrv.Web.Services
{
    public class MontrgService : IMontrgService
    {
        #region Fields

        private readonly IDeviceDataService _deviceDs;
        private readonly ICfgDataService _cfgDs;
        private readonly IImrDataService _imrDs;
        private readonly IEvntLogDataService _evntLogDs;
        private readonly IMailingAddrDataService _mailAddrDs;
        private readonly ILoggerManager _logger;

        private readonly IDeviceCtrlDataService _deviceCtrlDs;
        private readonly IFrameCtrlDataService _frameCtrlDs;
        private readonly IRoiCtrlDataService _roiCtrlDs;

        //private readonly MontrgEngine _engine;
        private System.Timers.Timer _timer;
#if !DEBUG
        private const int INTERVAL_MINUTE = 10;
#else
        private const int INTERVAL_MINUTE = 1;
#endif

        #endregion

        #region Constructors

        public MontrgService(IDeviceCtrlDataService deviceCtrlDs,
                             IFrameCtrlDataService frameCtrlDs,
                             IRoiCtrlDataService roiCtrlDs,
                             IDeviceDataService deviceDs,
                             ICfgDataService cfgDs,
                             IImrDataService imrDs,
                             IEvntLogDataService evntLogDs,
                             IMailingAddrDataService mailAddrDs,
                             ILoggerManager logger)
        {
            _deviceDs = deviceDs;
            _cfgDs = cfgDs;
            _imrDs = imrDs;
            _evntLogDs = evntLogDs;
            _mailAddrDs = mailAddrDs;
            _logger = logger;

            //_engine = new MontrgEngine(deviceCtrlDs, frameCtrlDs, roiCtrlDs);
            _deviceCtrlDs = deviceCtrlDs;
            _frameCtrlDs = frameCtrlDs;
            _roiCtrlDs = roiCtrlDs;

            InitTimer();
        }

        #endregion

        #region Public Methods

#if !DEBUG
        public void Start()
        {
            _timer.Start();
        }
#else
        public async void Start()
        {
            await ProcessAsync();
        }
#endif

        public void Stop()
        {
            _timer.Stop();
        }

        #endregion

        #region Private Methods

        private void InitTimer()
        {
            _timer = new System.Timers.Timer { Interval = TimeSpan.FromMinutes(INTERVAL_MINUTE).TotalMilliseconds };
            _timer.Elapsed += async (s, e) =>
            {
                try
                {
                    await ProcessAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex.ToFormattedString());
                }
            };
        }

        private async Task ProcessAsync()
        {
            _logger.LogInfo($">>> ProcessAsync() started");

            var eventQueue = new ConcurrentQueue<EventData>();
            var devices = await _deviceDs.GetList();

            // Tuple<deviceId, result>
            List<Task<Tuple<string, bool>>> tasks = new();

            if (devices == null)
            {
                _logger.LogError($"devices is null. the data api service may not be working.");
                throw new ArgumentNullException(nameof(devices));
            }

            foreach (var device in devices)
            {
                // Parallel processing
                var task = Task<Tuple<string, bool>>.Run(async () =>
                {
                    var deviceId = device.DeviceId;
                    var plantId = device.PlantId;

                    try
                    {
                        // Define data time range by plant
                        string start = null;
                        string end = null;

                        if (plantId == Plant.JJ.ToString() || plantId == Plant.CKP.ToString() || plantId == Plant.JJS.ToString() || plantId == Plant.RJ.ToString())
                        {
#if !DEBUG
                            // Indonesia time (GMT+7)
                            start = DateTime.Now.AddHours(-4).ToYmdHms();
                            end = DateTime.Now.AddHours(-1).ToYmdHms();
#else
                            //start = DateTime.Now.AddHours(-2).ToYmdHms();
                            //end = DateTime.Now.ToYmdHms();
                            start = new DateTime(2021, 8, 31, 15, 0, 0).ToYmdHms();
                            end = new DateTime(2021, 8, 31, 18, 0, 0).ToYmdHms();
#endif
                        }
                        else if (plantId == Plant.HQ.ToString())
                        {
                            // Korea time (GMT+9)
                            start = DateTime.Now.AddHours(-2).ToYmdHms();
                            end = DateTime.Now.ToYmdHms();
                        }
                        else if (plantId == Plant.VJ.ToString())
                        {
                            // Vietnam time (GMT+7)
                            start = DateTime.Now.AddHours(-4).ToYmdHms();
                            end = DateTime.Now.AddHours(-1).ToYmdHms();
                        }
                        else if (plantId == Plant.QD.ToString())
                        {
                            // China time (GMT+8)
                            start = DateTime.Now.AddHours(-3).ToYmdHms();
                            end = DateTime.Now.ToYmdHms();
                        }

                        // Get summarized statistic data
                        var imrData = await _imrDs.GetFrameImrData(deviceId, start, end);

                        // Skip data duplication
                        if (imrData != null)
                        {
                            if (imrData.Count == 0)
                                return new Tuple<string, bool>(deviceId, true);

                            var last = imrData.Last();
                            var has = await _evntLogDs.Exists(deviceId, last.Dt.Value);
                            if (has)
                                return new Tuple<string, bool>(deviceId, true);
                        }

                        // Validate the frame
                        var frameEngine = new MontrgEngine(_deviceCtrlDs, _frameCtrlDs, _roiCtrlDs);
                        var frmEvtlist = await frameEngine.Validate(imrData, device, InspArea.Frame);
                        frmEvtlist.ForEach(x => eventQueue.Enqueue(x));

                        // Validate ROIs
                        var rois = await _cfgDs.GetCfgRois(deviceId);
                        foreach (var roi in rois)
                        {
                            int roiId = roi.Key;
                            imrData = await _imrDs.GetRoiImrData(roiId, deviceId, start, end);
                            var roiEngine = new MontrgEngine(_deviceCtrlDs, _frameCtrlDs, _roiCtrlDs);
                            var roiEvtlist = await roiEngine.Validate(imrData, device, InspArea.ROI, roiId);
                            roiEvtlist.ForEach(x => eventQueue.Enqueue(x));
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex.ToFormattedString());
                        return new Tuple<string, bool>(deviceId, false);
                    }

                    return new Tuple<string, bool>(deviceId, true);
                });
                tasks.Add(task);
            }

            while (tasks.Count > 0)
            {
                Task<Tuple<string, bool>> finishedTask = tasks[Task.WaitAny(tasks.ToArray())];
                if (finishedTask.Result.Item2 == false)
                {
                    _logger.LogWarn($">>> [MontrgService] Failed to validate data: {finishedTask.Result.Item1}");
                }
                tasks.Remove(finishedTask);
            }

            //Debug.WriteLine($"---{start} ~ {end}--- All Event Data ---------------------------");
            eventQueue.ForEach(x => Debug.WriteLine(x.ToString()));

            //Debug.WriteLine($"---{start} ~ {end}--- Filtered Event Data ---------------------------");
            //var filtered = eventQueue.Where(x => x.EventLevel == EventLevel.Urgent ||
            //                                     x.EventLevel == EventLevel.Warning);
            //filtered.ForEach(x => Debug.WriteLine(x.ToString()));

            // Remove duplicated events
            List<EventData> list = eventQueue.GroupBy(x => x.EventId)
                                             .Select(g => g.First())
                                             .OrderBy(x => x.Id).ThenBy(x => x.EvntDt)
                                             .ToList();

            // Save and send emails
            await Save(list);
        }

        private async Task<bool> Save(List<EventData> eventData)
        {
            int totalSaved = 0;
#if !DEBUG
            foreach (var data in eventData)
            {
                // Store into DB
                var r = await _evntLogDs.Create(data.ToEventLog());
                if (r != null)
                {
                    var rId = $"{r.PlantId}.{r.LocationId}.{r.DeviceId}.{r.InspArea}{r.AreaId} @ {r.Ymd}_{r.Hms}";
                    Debug.WriteLine($">>> Saved: {rId}");
                    totalSaved++;
                }
                else
                {
                    var rId = $"{data.Device.PlantId}.{data.Device.LocationId}.{data.Device.DeviceId}." +
                              $"{data.InspArea}{data.AreaId} @ {data.EvntDt.ToYmdHmsWithSeparator()}";
                    _logger.LogWarn($">>> Failed to save: {rId}");
                }
            }
#else
            await Task.Delay(1);
#endif

#if !DEBUG
            SendEmail(eventData);

            if (totalSaved == eventData.Count)
                return true;
#endif

            return false;
        }


        private async void SendEmail(List<EventData> eventData)
        {
            // Server setting
            var sender = new SmtpSender(() => new SmtpClient()
            {
                Host = AppSettingsProvider.EmailAccountSmtpHost,
                Port = AppSettingsProvider.EmailAccountSmtpPort,
                Credentials = new NetworkCredential(AppSettingsProvider.EmailAccountId,
                                                    AppSettingsProvider.EmailAccountPassword),
                EnableSsl = false,
            });
            Email.DefaultSender = sender;
            Email.DefaultRenderer = new FluentEmail.Razor.RazorRenderer();

            var emailingAddrs = await _mailAddrDs.GetActiveList();

#if !DEBUG
            // Warning or Urgent events
            var data = eventData.Where(x => x.EventLevel == EventLevel.Warning || x.EventLevel == EventLevel.Urgent);
#else
            // All events
            var data = eventData;
#endif

            if (data.Any())
            {
                var plantIds = data.Select(x => x.Device.PlantId).Distinct();

                foreach (var plantId in plantIds)
                {
                    try
                    {
                        var events = data.Where(x => x.Device.PlantId == plantId)
                                     .OrderByDescending(x => x.EventLevel)
                                     .ToList();
                        var deviceDic = events.GroupBy(x => x.Device.DeviceId)
                                              .ToDictionary(grp => grp.Key, grp => grp.First().Device);
                        var emailAddrs = emailingAddrs.Where(x => x.PlantId == plantId);

                        // Send email
                        IFluentEmail email = Email.From(AppSettingsProvider.EmailAccountId, "TR Monitoring Solution");
                        var emailSender = new EmailSender(email, _logger);
                        var result = await emailSender.SendUsingTemplateFromEmbedded(
                                                            emailAddrs.Select(x => new Address { EmailAddress = x.Email, Name = x.Name }),
                                                            new List<Address> {
                                                                new Address {
                                                                    EmailAddress = AppSettingsProvider.EmailAccountId,
                                                                    Name = "MI Team"
                                                                }
                                                            },
                                                            $"[TR Monitoring] - {plantId}",
                                                            EmailTemplate.Alert,
                                                            new { Data = events, DeviceDic = deviceDic });
                        if (result == false)
                        {
                            _logger.LogWarn($">>> Failed to send an alert email: {plantId}");
                        }
                        else
                        {
                            var deviceIds = String.Join(",", events.Select(x => x.Device.DeviceId).Distinct());
                            _logger.LogInfo($"Sent an alert email: {plantId}, Devices: {deviceIds}, Alerts: {events.Count}");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($">>> Failed to send alert email to {plantId}: {ex.ToFormattedString()}");
                    }
                }
            }
        }

#endregion
    }
}
