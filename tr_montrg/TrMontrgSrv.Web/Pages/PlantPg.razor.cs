using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Routing;
using Microsoft.JSInterop;

namespace CSG.MI.TrMontrgSrv.Web.Pages
{
    public partial class PlantPg : IDisposable
    {
        #region Fields

        protected int TotalUrgents;

        protected int TotalWarnings;

        #endregion

        #region Properties

        //[Inject]
        //public HttpClient HttpClient { get; set; }

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IEvntLogDataService EvntLogDataService { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        public List<EvntLog> EvntLogs { get; set; }

        #endregion

        #region Override Methods

        protected override Task OnInitializedAsync()
        {
            //await LoadDataAsync();

            //NavManager.LocationChanged += LocationChanged;

            return base.OnInitializedAsync();
        }

        protected override Task OnAfterRenderAsync(bool firstRender)
        {
            return base.OnAfterRenderAsync(firstRender);
        }

        #endregion

        #region Private Methods

        private async Task LoadDataAsync()
        {
            if (String.IsNullOrEmpty(PlantId))
                return;

            EvntLogs = await EvntLogDataService.GetLatest(PlantId, excludingInfoLevel:true);

            if (EvntLogs != null)
            {
                TotalUrgents = EvntLogs.Count(x => x.EvntLevel == EventLevel.Urgent.ToString());
                TotalWarnings = EvntLogs.Count(x => x.EvntLevel == EventLevel.Warning.ToString());
            }
        }

        private async void LocationChanged(object sender, LocationChangedEventArgs e)
        {
            await LoadDataAsync();
        }

        #endregion

        public void Dispose()
        {
            // Unsubscribe from the event when our component is disposed
            //NavManager.LocationChanged -= LocationChanged;
        }
    }
}
