using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Infrastructure;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.JSInterop;

namespace CSG.MI.TrMontrgSrv.Web.Pages
{
    public partial class MaxPosPg
    {
        #region Fields

        private IEnumerable<Model.Frame> _frames = null;

        #endregion

        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IFrameDataService FrameDataService { get; set; }

        [Inject]
        public IAppUiCfgProvider AppUiCfgProvider { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        [Parameter]
        public string LocationId { get; set; }

        [Parameter]
        public string DeviceId { get; set; }

        public string ModalTitle { get; set; }

        public string ModalMessage { get; set; }

        #endregion

        #region Override Methods

        protected override async Task OnAfterRenderAsync(bool firstRender)
        {
            if (firstRender)
            {
                await JsRuntime.InvokeVoidAsync("blazorInterop.initDateRangePicker");
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
            _frames = null;

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
                _frames = await FrameDataService.GetList(DeviceId, start, end);
            }
            catch
            {
                OpenMessageModal("Error", "Failed to fetch data.");
                await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");
                return;
            }

            await JsRuntime.InvokeAsync<Task>("blazorInterop.drawMaxPosChart", "maxPosChart", _frames, "maxPosTable");
            await JsRuntime.InvokeVoidAsync("blazorInterop.hideSpinner");

            if (_frames.Count() == 0)
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
