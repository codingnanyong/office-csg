using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;

namespace CSG.MI.TrMontrgSrv.Web.Pages
{
    public partial class RoiPg
    {
        // date range picker
        // https://github.com/dangrossman/daterangepicker/
        // change chart line color base on value
        // https://stackoverflow.com/questions/50034765/multi-colored-line-chart-with-google-visualization
        // https://stackoverflow.com/questions/40150907/google-line-chart-change-color-when-line-down
        // https://stackoverflow.com/questions/46596229/google-column-graphs-set-color-conditionally
        // variable background colors in google line chart
        // https://stackoverflow.com/questions/39669997/variable-background-colors-in-google-line-chart
        // How to detect a full page Refresh/Reload in Blazor
        // https://stackoverflow.com/questions/60085858/how-to-detect-a-full-page-refresh-reload-in-blazor

        #region Fields

        private Dictionary<int, int[]> _cfgRoi = null;
        private IEnumerable<ImrData> _imrData = null;

        #endregion

        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IImrDataService ImrDataService { get; set; }

        [Inject]
        public ICfgDataService CfgDataService { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        [Parameter]
        public string LocationId { get; set; }

        [Parameter]
        public string DeviceId { get; set; }

        public string RoiId { get; set; }

        public List<int> RoiIds { get; set; } = new();

        public string ModalTitle { get; set; }

        public string ModalMessage { get; set; }

        #endregion

        #region Override Methods

        protected override async Task OnInitializedAsync()
        {
            try
            {
                _cfgRoi = await CfgDataService.GetCfgRois(DeviceId);
            }
            catch
            {
                OpenMessageModal("Error", "Failed to fetch data.");
                await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                return;
            }

            foreach (var kvp in _cfgRoi)
            {
                RoiIds.Add(kvp.Key);
            }
            RoiIds = RoiIds.OrderBy(x => x).ToList(); // Order by RoiId

            //System.Diagnostics.Debug.WriteLine("---> OnInitialized()");
        }

        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (firstRender)
            {
                await JsRuntime.InvokeVoidAsync("blazorInterop.initDateRangePicker");
                RoiId = "0";
#if !DEBUG
                await LoadChartAsync(null);
#endif
                //System.Diagnostics.Debug.WriteLine("---> OnAfterRenderAsync(): FirstRender");
            }
            else
            {
                //System.Diagnostics.Debug.WriteLine("---> OnAfterRenderAsync(): Not firstRender");
            }
        }

        #endregion

        #region Private Methods

        private async Task LoadChartAsync(MouseEventArgs e)
        {
            await JsRuntime.InvokeVoidAsync("blazorInterop.showSpinner");
            _imrData = null;

            if (String.IsNullOrEmpty(DeviceId))
            {
                OpenMessageModal("Alert", "Device ID is missing.");
                await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                return;
            }

            int roiId = -1;
            bool parsed = Int32.TryParse(RoiId, out roiId);
            if (parsed == false || roiId < 0)
            {
                OpenMessageModal("Information", "Please select one ROI ID.");
                await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                return;
            }

            string timeRange = await JsRuntime.InvokeAsync<string>("blazorInterop.getInputValue", "timeRange");
            //System.Diagnostics.Debug.WriteLine($"----> ROI_ID:{RoiId}, TIME:{timeRange}");
            int idxSep = timeRange.IndexOf('-');
            string startDtString = timeRange.Substring(0, idxSep).Trim();
            string endDtString = timeRange.Substring(idxSep + 1).Trim();

            DateTime startDt;
            DateTime endDt;
            parsed = DateTime.TryParse(startDtString, out startDt);
            parsed = DateTime.TryParse(endDtString, out endDt);
            if (parsed == false)
            {
                OpenMessageModal("Alert", "Failed to parse the Time Range.");
                await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                return;
            }

            string start = startDt.ToYmdHmsWithSeparator();
            string end = endDt.ToYmdHmsWithSeparator();
            try
            {
                _imrData = await ImrDataService.GetRoiImrData(roiId, DeviceId, start, end);
            }
            catch
            {
                OpenMessageModal("Error", "Failed to fetch data.");
                await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                return;
            }

            var url = $"{ImrDataService.Host}/api/v{ImrDataService.Version}/snap";
            await JsRuntime.InvokeAsync<Task>("blazorInterop.drawCtrChart", "iMaxChart", _imrData, "i-max", url, DeviceId);
            await JsRuntime.InvokeAsync<Task>("blazorInterop.drawCtrChart", "mrMaxChart", _imrData, "mr-max", url, DeviceId);
            await JsRuntime.InvokeAsync<Task>("blazorInterop.drawCtrChart", "iDiffChart", _imrData, "i-diff", url, DeviceId);
            await JsRuntime.InvokeAsync<Task>("blazorInterop.drawCtrChart", "mrDiffChart", _imrData, "mr-diff", url, DeviceId);

            await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");

            if (_imrData.Count() == 0)
            {
                OpenMessageModal("Result", "No data found.");
                await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                return;
            }
        }

        private async void OpenMessageModal(string title, string message)
        {
            ModalTitle = title;
            ModalMessage = message;
            await JsRuntime.InvokeAsync<Task>("blazorInterop.showModal", "modalWindow", title, message);
        }

        #endregion

    }
}
