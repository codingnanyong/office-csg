using CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared.Modals;
using CSG.MI.TrMontrgSrv.Dashboard.Infrastructure;
using CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
using Microsoft.AspNetCore.Components;

namespace CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared
{
    public partial class BasePlant
    {
        #region Fields

        private ModalDialog? modal;

        #endregion

        #region Properties

        [Parameter]
        public string Factory { get; set; } = string.Empty;

        [Parameter]
        public List<CurDevice>? Devices { get; set; }

        #endregion

        #region Injects

        [Inject]
        public required IDashboardDataService DashboardDataService { get; set; }

        [Inject]
        private CommonHelper? _common { get; set; }

        #endregion

        #region Lifecycle Methods

        protected override async Task OnInitializedAsync()
        {
            await LoadDevicesAsync();
        }

        #endregion

        #region Private Methods

        private async Task LoadDevicesAsync()
        {
            Devices = await DashboardDataService.GetDevicesByFactory(Factory);
        }

        #endregion

        #region Private Methods : Contents

        private async void ShowModal(CurDevice device)
        {
            if (device != null)
            {
                modal!.SetModal(device!);
                await modal!.OnShowModal();
            }
        }

        private string SetCardHeaderId(CurDevice device)
        {
            return _common!.SetCardHeaderId(device);
        }

        private string absContext(float? tmp)
        {
            return _common!.absContext(tmp);
        }

        private string SetIcon(float? tmp)
        {
            return _common!.SetIcon(tmp);
        }

        #endregion
    }
}
