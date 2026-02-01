using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Web.Components;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;
using Blazor.Analytics.Abstractions;

namespace CSG.MI.TrMontrgSrv.Web.Pages.Admin
{
    public partial class DeviceOverview
    {
        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IDeviceDataService DeviceDataService { get; set; }

        [Inject]
        private ITrackingNavigationState TrackingNavigationState { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        public IEnumerable<Model.Device> Devices { get; set; }

        protected DeviceAddDialog DeviceAddDialog { get; set; }

        #endregion

        protected override async Task OnInitializedAsync()
        {
            TrackingNavigationState.DisableTracking();

            await LoadData();
        }

        protected void ShowDeviceAddDialog()
        {
            DeviceAddDialog.Show();
        }

        protected async void DeviceAddDialog_OnDialogClose()
        {
            await LoadData();
            StateHasChanged();
        }

        protected void NavigateToAdminHome()
        {
            NavManager.NavigateTo($"/admin");
        }

        private async Task LoadData()
        {
            Devices = await DeviceDataService.GetList(PlantId);
            Devices = Devices?.OrderBy(x => x.PlantId).ThenBy(x => x.Order).ThenBy(x => x.LocationId);
        }
    }
}
