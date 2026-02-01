using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.JSInterop;
using Blazor.Analytics.Abstractions;

namespace CSG.MI.TrMontrgSrv.Web.Pages.Admin
{
    public partial class BoxCtrlEdit
    {
        #region Fields

        protected bool Saved;
        protected string Message = String.Empty;
        protected string StatusClass = String.Empty;
        protected ElementReference DeviceIdInput;
        protected bool ShowSpinner;
        protected bool IsAddMode;

        #endregion

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

        public BoxCtrl BoxCtrl { get; set; } = new BoxCtrl();

        #endregion

        protected override async Task OnInitializedAsync()
        {
            TrackingNavigationState.DisableTracking();

            if (String.IsNullOrEmpty(DeviceId))
            {
                BoxCtrl = new BoxCtrl();

                var uri = NavManager.ToAbsoluteUri(NavManager.Uri);
                if (QueryHelpers.ParseQuery(uri.Query).TryGetValue("deviceId", out var _deviceId))
                {
                    IsAddMode = true;
                    BoxCtrl.DeviceId = _deviceId;
                    DeviceId = _deviceId;
                }
            }
            else
            {
                BoxCtrl = await BoxCtrlDataService.Get(DeviceId);
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

            //if (String.IsNullOrEmpty(DeviceId)) // New
            if (IsAddMode)
            {
                var itemCreated = await BoxCtrlDataService.Create(BoxCtrl);
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
                var success = await BoxCtrlDataService.Update(BoxCtrl);
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
            var success = await BoxCtrlDataService.Delete(BoxCtrl.DeviceId);
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
            NavManager.NavigateTo($"/admin/boxctrls/{PlantId}/{DeviceId}");
        }
    }
}
