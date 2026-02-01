using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;
using Blazor.Analytics.Abstractions;

namespace CSG.MI.TrMontrgSrv.Web.Pages.Admin
{
    public partial class GrpKeyEdit
    {
        #region Fields

        protected bool Saved;
        protected string Message = String.Empty;
        protected string StatusClass = String.Empty;
        protected ElementReference DeviceIdInput;
        protected bool ShowSpinner;

        #endregion

        #region Properties

        [Inject]
        public NavigationManager NavManager { get; set; }

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IGrpKeyDataService GrpKeyDataService { get; set; }

        [Inject]
        private ITrackingNavigationState TrackingNavigationState { get; set; }

        [Parameter]
        public string Group { get; set; }

        [Parameter]
        public string Key { get; set; }

        public GrpKey GrpKey { get; set; } = new GrpKey();

        #endregion

        protected override async Task OnInitializedAsync()
        {
            TrackingNavigationState.DisableTracking();

            if (String.IsNullOrEmpty(Group))
            {
                GrpKey = new GrpKey();
            }
            else
            {
                GrpKey = await GrpKeyDataService.Get(Group, Key);
            }
        }

        //protected override async Task OnAfterRenderAsync(bool firstRender)
        //{
        //    if (firstRender)
        //    {
        //        await JsRuntime.InvokeVoidAsync("blazorInterop.focusElementById", "deviceId");
        //    }
        //}

        protected async Task HandleValidSubmit()
        {
            Saved = false;
            ShowSpinner = true;

            if (String.IsNullOrEmpty(Group))
            {
                var itemCreated = await GrpKeyDataService.Create(GrpKey);
                if (itemCreated != null)
                {
                    StatusClass = "alert-success";
                    Message = "New item added successfully.";
                }
                else
                {
                    StatusClass = "alert-danger";
                    Message = "Failed to add the item. Please try again.";
                }
            }
            else
            {
                var success = await GrpKeyDataService.Update(GrpKey);
                if (success)
                {
                    StatusClass = "alert-success";
                    Message = "The item updated successfully.";
                }
                else
                {
                    StatusClass = "alert-danger";
                    Message = "Failed to update the item.";
                }
            }

            Saved = true;
            ShowSpinner = false;
        }

        protected void HandleInvalidSubmit()
        {
            StatusClass = "alert-danger";
            Message = "Validation errors. Please try again.";
        }

        protected async Task Delete()
        {
            var success = await GrpKeyDataService.Delete(GrpKey.Group, GrpKey.Key);
            if (success)
            {
                StatusClass = "alert-success";
                Message = "Deleted successfully.";
            }
            else
            {
                StatusClass = "alert-danger";
                Message = "Failed to delete.";
            }

            Saved = true;
        }

        protected void NavigateToOverview()
        {
            NavManager.NavigateTo($"/admin/grpkeys/");
        }
    }
}
