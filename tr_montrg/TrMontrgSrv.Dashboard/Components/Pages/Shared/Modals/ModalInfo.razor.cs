using BlazorBootstrap;
using CSG.MI.TrMontrgSrv.Dashboard.Infrastructure;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
using Microsoft.AspNetCore.Components;

namespace CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared.Modals
{
    public partial class ModalInfo
    {
        #region Fields

        public Modal modal = default!;

        public string currentModalContent = nameof(ModalInfo1);

        #endregion

        #region Public Methods

        public async Task NextModalContent()
        {
            switch (currentModalContent)
            {
                case nameof(ModalInfo1):
                    currentModalContent = nameof(ModalInfo2);
                    break;
            }
            await Task.CompletedTask;
        }

        public async Task PreviusModalContent()
        {
            switch (currentModalContent)
            {
                case nameof(ModalInfo2):
                    currentModalContent = nameof(ModalInfo1);
                    break;
            }
            await Task.CompletedTask;
        }

        public async Task OnShowModal()
        {
            await modal.ShowAsync();
        }

        public async Task OnHideModal()
        {
            await modal.HideAsync();
            currentModalContent = nameof (ModalInfo1);
        }

        public string SetModalId() => currentModalContent switch
        {
            nameof(ModalInfo1) => "modal-info-1",
            nameof(ModalInfo2) => "modal-info-2",
            _ => ""
        };

        #endregion

    }
}
