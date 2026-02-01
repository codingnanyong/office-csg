using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Components;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;
using Blazor.Analytics.Abstractions;

namespace CSG.MI.TrMontrgSrv.Web.Pages.Admin
{
    public partial class RoiCtrlOverview
    {
        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IRoiCtrlDataService RoiCtrlDataService { get; set; }

        [Inject]
        private ITrackingNavigationState TrackingNavigationState { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        [Parameter]
        public string DeviceId { get; set; }

        public List<RoiCtrl> RoiCtrls { get; set; }

        public bool IsDataLoaded { get; set; }

        protected RoiCtrlAddDialog RoiCtrlAddDialog { get; set; }

        #endregion

        protected override async Task OnInitializedAsync()
        {
            TrackingNavigationState.DisableTracking();

            await LoadData();
        }

        protected void ShowRoiCtrlAddDialog()
        {
            RoiCtrlAddDialog.Show();
        }

        protected void NavigateToDevices()
        {
            NavManager.NavigateTo($"/admin/devices/{PlantId}");
        }

        protected async void RoiCtrlAddDialog_OnDialogClose()
        {
            await LoadData();
            StateHasChanged();
        }

        private async Task LoadData()
        {
            IsDataLoaded = false;
            RoiCtrls = await RoiCtrlDataService.GetList(DeviceId);
            IsDataLoaded = true;
        }


    }
}
