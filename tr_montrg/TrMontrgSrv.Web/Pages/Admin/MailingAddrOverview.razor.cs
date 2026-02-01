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
    public partial class MailingAddrOverview
    {
        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IMailingAddrDataService MailingAddrDataService { get; set; }

        [Inject]
        private ITrackingNavigationState TrackingNavigationState { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        public List<MailingAddr> MailingAddrs { get; set; }

        public bool IsDataLoaded { get; set; }

        protected MailingAddrAddDialog MailingAddrAddDialog { get; set; }

        #endregion

        protected override async Task OnParametersSetAsync()
        {
            TrackingNavigationState.DisableTracking();

            await LoadData(PlantId);
            //return base.OnParametersSetAsync();
        }

        //protected override async Task OnInitializedAsync()
        //{
        //    await LoadData(PlantId);
        //}

        protected void ShowMailingAddrAddDialog()
        {
            MailingAddrAddDialog.Show();
        }

        protected void NavigateToAdminHome()
        {
            NavManager.NavigateTo($"/admin");
        }

        protected async void MailingAddrDialog_OnDialogClose()
        {
            await LoadData(PlantId);
            StateHasChanged();
        }

        private async Task LoadData(string plantId)
        {
            IsDataLoaded = false;
            MailingAddrs = await MailingAddrDataService.GetList(plantId);
            IsDataLoaded = true;
        }


    }
}
