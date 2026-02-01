using BlazorBootstrap;
using CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared;
using CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared.Modals;
using CSG.MI.TrMontrgSrv.Dashboard.Infrastructure;
using Microsoft.AspNetCore.Components;

namespace CSG.MI.TrMontrgSrv.Dashboard.Components.Layout
{
    public partial class MainLayout
    {
        #region Fields

        private ModalInfo? modal;

        private ModalRequest? modal_request;

        #endregion

        #region Public Methods

        private async void ShowInfoModal()
        {
            await modal!.OnShowModal();
        }

        private async void ShowRequestModal()
        {
            await modal_request!.OnShowModal();
        }

        #endregion
    }
}
