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
    public partial class GrpKeyOverview
    {
        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IGrpKeyDataService GrpKeyDataService { get; set; }

        [Inject]
        private ITrackingNavigationState TrackingNavigationState { get; set; }

        public List<GrpKey> GrpKeys { get; set; }

        public bool IsDataLoaded { get; set; }

        protected GrpKeyAddDialog GrpKeyAddDialog { get; set; }

        #endregion

        protected override async Task OnInitializedAsync()
        {
            TrackingNavigationState.DisableTracking();

            await LoadData();
        }

        protected void ShowGrpKeyAddDialog()
        {
            GrpKeyAddDialog.Show();
        }

        protected void NavigateToAdminHome()
        {
            NavManager.NavigateTo($"/admin");
        }

        protected async void GrpKeyAddDialog_OnDialogClose()
        {
            await LoadData();
            StateHasChanged();
        }

        private async Task LoadData()
        {
            IsDataLoaded = false;
            GrpKeys = await GrpKeyDataService.GetList();
            IsDataLoaded = true;
        }


    }
}
