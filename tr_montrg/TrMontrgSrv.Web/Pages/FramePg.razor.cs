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
    public partial class FramePg
    {
        #region Fields

        private IEnumerable<ImrData> _imrData = null;

        #endregion

        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IImrDataService ImrDataService { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        [Parameter]
        public string LocationId { get; set; }

        [Parameter]
        public string DeviceId { get; set; }

        public string ModalTitle { get; set; }

        public string ModalMessage { get; set; }

        public string TimeRange { get; set; }

        #endregion

        #region Override Methods

        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            //await base.OnAfterRenderAsync(firstRender);
            //await JsRuntime.InvokeAsync<Task>("initDateRangePicker");

            if (firstRender)
            {
                try
                {
                    await JsRuntime.InvokeVoidAsync("blazorInterop.initDateRangePicker");
                }
                catch (JSException jsex)
                {
                    OpenMessageModal("Error", jsex.Message);
                }
#if !DEBUG
                await LoadChartAsync(null);
#endif
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

            string timeRange = await JsRuntime.InvokeAsync<string>("blazorInterop.getInputValue", "timeRange");
            System.Diagnostics.Debug.WriteLine($"----> TIME:{timeRange}");
            int idxSep = timeRange.IndexOf('-');
            string startDtString = timeRange.Substring(0, idxSep).Trim();
            string endDtString = timeRange.Substring(idxSep + 1).Trim();

            DateTime startDt;
            DateTime endDt;
            var parsed = DateTime.TryParse(startDtString, out startDt);
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
                _imrData = await ImrDataService.GetFrameImrData(DeviceId, start, end);
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

            if (_imrData.Any() == false)
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
            await JsRuntime.InvokeVoidAsync("blazorInterop.showModal", "modalWindow", title, message);
        }

        #endregion

    }
}
