using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace CSG.MI.TrMontrgSrv.Web.Components
{
    public partial class DeviceAddDialog
    {
        #region Fields

        protected bool Saved;
        protected string Message = String.Empty;
        protected string StatusClass = String.Empty;
        protected bool ShowSpinner;

        #endregion

        #region Properties

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IDeviceDataService DeviceDataService { get; set; }

        [Parameter]
        public string PlantId { get; set; }

        //[Parameter]
        //public string DeviceId { get; set; }

        [Parameter]
        public EventCallback<bool> CloseEventCallback { get; set; }

        public Model.Device Device { get; set; } = new Model.Device();

        public bool ShowDialog { get; set; }

        #endregion

        //protected override async Task OnAfterRenderAsync(bool firstRender)
        //{
        //    if (ShowDialog)
        //        await JsRuntime.InvokeVoidAsync("blazorInterop.focusElementById", "deviceId");
        //}

        protected async Task HandleValidSubmit()
        {
            Saved = false;
            ShowSpinner = true;

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

            Saved = true;
            ShowSpinner = false;
        }

        protected void HandleInvalidSubmit()
        {
            StatusClass = "alert-danger";
            Message = "Validation errors. Please try again.";
        }

        public void Show()
        {
            Saved = false;
            ResetDialog();
            ShowDialog = true;
            StateHasChanged();
        }

        public async Task Close()
        {
            ShowDialog = false;
            if (Saved)
            {
                await CloseEventCallback.InvokeAsync(true);
            }
            Saved = false;
            StateHasChanged();
        }

        private void ResetDialog()
        {
            Device = new Model.Device() { PlantId = PlantId };
        }
    }
}
