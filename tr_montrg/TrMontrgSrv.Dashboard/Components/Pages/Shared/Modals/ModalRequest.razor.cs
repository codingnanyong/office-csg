using BlazorBootstrap;
using CSG.MI.DTO.Feedback;
using CSG.MI.TrMontrgSrv.Dashboard.Infrastructure;
using CSG.MI.TrMontrgSrv.Dashboard.Resources;
using CSG.MI.TrMontrgSrv.Dashboard.Services.Dashboard.Interface;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.Localization;

namespace CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared.Modals
{
    public partial class ModalRequest
    {
        #region Fields

        public Modal modal = default!;

        public string selected_category = "";

        #endregion

        #region Properties

        [Inject]
        public IFdwDataService? FdwDataService { get; set; }

        [Inject]
        public IStringLocalizer<language>? Language { get; set; }

        [Parameter]
        public ICollection<Category>? Categories { get; set; }

        private Feedback Feedback { get; set; } = new Feedback { Seq = null, System = "2", Comment = "" };

        #endregion

        #region Lifecycle Methods

        protected override async Task OnInitializedAsync()
        {
            await base.OnInitializedAsync();

            selected_category = Language!["request_dropdown"].Value;

            await LoadCategoriesAsnyc();

        }

        #endregion

        #region Public Methods : Modal

        public async Task OnShowModal()
        {
            await modal.ShowAsync();
        }

        public async Task OnHideModal()
        {
            ResetRequest();
            await modal.HideAsync();
        }

        #endregion

        #region Private Methods

        private void UpdateSelectedCategory(Category category)
        {
            selected_category = category.Value;
            Feedback.Category = category.Id;
        }

        private void ResetRequest()
        {
            selected_category = Language!["request_dropdown"].Value;
            Feedback.Comment = "";
        }

        private async Task LoadCategoriesAsnyc()
        {
            await Task.Delay(500);
            try
            {
                if (FdwDataService == null)
                {
                    Console.WriteLine("FdwDataService is null.");
                }
                var api = Language!["request_api"].Value;

                Categories = FdwDataService != null ? await FdwDataService.GetCategories(api) : null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private async Task SendRequest()
        {
            try
            {
                if (FdwDataService == null)
                {
                    Console.WriteLine("FdwDataService is null.");
                    return;
                }

                await FdwDataService.SendRequest(Feedback);
                await OnHideModal();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        #endregion
    }
}
