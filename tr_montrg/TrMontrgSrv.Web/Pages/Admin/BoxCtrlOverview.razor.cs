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
    public partial class BoxCtrlOverview
    {
        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IBoxCtrlDataService BoxCtrlDataService { get; set; }

        [Inject]
        private ITrackingNavigationState TrackingNavigationState { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        [Parameter]
        public string DeviceId { get; set; }

        public BoxCtrl BoxCtrl { get; set; }

        public bool IsDataLoaded { get; set; }

        protected BoxCtrlAddDialog BoxCtrlAddDialog { get; set; }

        #endregion

        protected override async Task OnInitializedAsync()
        {
            TrackingNavigationState.DisableTracking();

            await LoadData();
        }

        protected void ShowBoxCtrlAddDialog()
        {
            BoxCtrlAddDialog.Show();
        }

        protected void NavigateToDevices()
        {
            NavManager.NavigateTo($"/admin/devices/{PlantId}");
        }

        protected async void BoxCtrlAddDialog_OnDialogClose()
        {
            await LoadData();
            StateHasChanged();
        }

        private async Task LoadData()
        {
            IsDataLoaded = false;
            BoxCtrl = await BoxCtrlDataService.Get(DeviceId);
            IsDataLoaded = true;
        }


    }
}
