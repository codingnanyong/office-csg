using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;
using Blazor.Analytics.Abstractions;

namespace CSG.MI.TrMontrgSrv.Web.Pages.Admin
{
    public partial class DeviceEdit
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
        public IDeviceDataService DeviceDataService { get; set; }

        [Inject]
        private ITrackingNavigationState TrackingNavigationState { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        [Parameter]
        public string DeviceId { get; set; }

        public Model.Device Device { get; set; } = new Model.Device();

        #endregion

        protected override async Task OnInitializedAsync()
        {
            TrackingNavigationState.DisableTracking();

            if (String.IsNullOrEmpty(DeviceId))
            {
                Device = new Model.Device() { PlantId = PlantId };
            }
            else
            {
                Device = await DeviceDataService.Get(DeviceId);
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

            if (String.IsNullOrEmpty(DeviceId)) // New
            {
                Device.UpdatedDt = DateTime.Now;
                var itemCreated = await DeviceDataService.Create(Device);
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
                Device.UpdatedDt = DateTime.Now;
                var success = await DeviceDataService.Update(Device);
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
            var success = await DeviceDataService.Delete(Device.DeviceId);
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
            NavManager.NavigateTo($"/admin/devices/{PlantId}");
        }
    }
}
