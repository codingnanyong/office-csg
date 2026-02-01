using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace CSG.MI.TrMontrgSrv.Web.Components
{
    public partial class GrpKeyAddDialog
    {
        #region Fields

        protected bool Saved;
        protected string Message = String.Empty;
        protected string StatusClass = String.Empty;
        protected bool ShowSpinner = false;

        #endregion

        #region Properties

        [Inject]
        public IJSRuntime JsRuntime { get; set; }

        [Inject]
        public IGrpKeyDataService GrpKeyDataService { get; set; }

        [Parameter]
        public EventCallback<bool> CloseEventCallback { get; set; }

        public GrpKey GrpKey { get; set; } = new GrpKey();

        public bool ShowDialog { get; set; }

        #endregion

        //protected override async Task OnAfterRenderAsync(bool firstRender)
        //{
        //    if (ShowDialog)
        //        await JsRuntime.InvokeVoidAsync("blazorInterop.focusElementById", "levelATo");
        //}

        protected async Task HandleValidSubmit()
        {
            Saved = false;
            ShowSpinner = true;

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
            GrpKey = new GrpKey();
        }
    }
}
