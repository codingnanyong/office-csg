using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using CSG.MI.TrMontrgSrv.LoggerService;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace CSG.MI.TrMontrgSrv.Web.Pages
{
    public partial class DevicePg
    {
        #region Fields

        protected int TotalUrgents;
        protected int TotalWarnings;
        protected int TotalInfos;

        private System.Timers.Timer _timer;
        private const int INTERVAL_MINUTE = 10;

        #endregion

        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IEvntLogDataService EvntLogDataService { get; set; }

        [Inject]
        public IDeviceDataService DeviceDataService { get; set; }

        [Inject]
        public ILoggerManager Logger { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        [Parameter]
        public string LocationId { get; set; }

        [Parameter]
        public string DeviceId { get; set; }

        public List<EvntLog> EvntLogs { get; set; }

        public Device Device { get; set; }

        #endregion

        #region Override Methods

        protected override async Task OnInitializedAsync()
        {
            await LoadDataAsync();
            InitTimer();
        }

        //protected override async Task OnAfterRenderAsync(bool firstRender)
        //{
        //    //return base.OnAfterRenderAsync(firstRender);

        //    //await JsRuntime.InvokeVoidAsync("blazorInterop.showSpinner");

        //    if (firstRender == false)
        //    {
        //        //await JsRuntime.InvokeVoidAsync("blazorInterop.showSpinner");
        //    }

        //    //await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");

        //    base.OnAfterRender(firstRender);
        //}

        #endregion

        #region Private Methods

        private async Task LoadDataAsync()
        {
            EvntLogs = await EvntLogDataService.GetLatest(PlantId, DeviceId);

            if (EvntLogs != null)
            {
                TotalUrgents = EvntLogs.Count(x => x.EvntLevel == EventLevel.Urgent.ToString());
                TotalWarnings = EvntLogs.Count(x => x.EvntLevel == EventLevel.Warning.ToString());
                TotalInfos = EvntLogs.Count(x => x.EvntLevel == EventLevel.Info.ToString());
            }

            Logger.LogInfo($"GetLastest({PlantId}, {DeviceId})");

            Device = await DeviceDataService.Get(DeviceId);
        }

        private void InitTimer()
        {
            _timer = new System.Timers.Timer { Interval = TimeSpan.FromMinutes(INTERVAL_MINUTE).TotalMilliseconds };
            _timer.Elapsed += async (s, e) =>
            {
                try
                {
                    Console.WriteLine($">>> Timer elapsed");
                    await JsRuntime.InvokeVoidAsync("blazorInterop.showSpinner");
                    await LoadDataAsync();
                    await InvokeAsync(() =>
                      {
                          StateHasChanged();
                      });
                    await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($">>> ERROR: {ex.Message}");
                }
            };
            _timer.Enabled = true;
        }

        private async void OpenPopupWindow()
        {
            await JsRuntime.InvokeVoidAsync("blazorInterop.openPopupWindowCenter",
                                            $"http://{Device.IpAddress}:{Device.UiPort}/",
                                            "Real-time",
                                            800,
                                            1000);
        }

        #endregion
    }
}
