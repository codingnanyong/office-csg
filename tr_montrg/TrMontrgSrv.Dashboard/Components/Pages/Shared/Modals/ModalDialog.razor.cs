using BlazorBootstrap;
using CSG.MI.TrMontrgSrv.Model.Dashboard;

namespace CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared.Modals
{
    public partial class ModalDialog
    {
        #region Fields
        public Modal modal = default!;

        private string Title { get; set; } = String.Empty;

        #endregion

        #region Public Methods

        public async Task OnShowModal()
        {
            await modal.ShowAsync();
        }

        public async Task OnHideModal()
        {
            await modal.HideAsync();
        }
        public void SetModal(CurDevice device)
        {
            Title = $"{device.Description}:{device.DeviceId}";
            StateHasChanged();
        }

        #endregion

        #region Private Methods

        #endregion
    }
}
