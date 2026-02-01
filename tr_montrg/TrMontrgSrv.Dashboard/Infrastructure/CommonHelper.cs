using CSG.MI.TrMontrgSrv.Dashboard.Components.Pages.Shared.Modals;
using CSG.MI.TrMontrgSrv.Model.Dashboard;

namespace CSG.MI.TrMontrgSrv.Dashboard.Infrastructure
{
    public class CommonHelper
    {
        #region Properties

        public static ModalDialog? modal;

        #endregion

        #region Public Methods - Component Id & Context

        public string SetCardHeaderId(CurDevice device)
        {
            if (device.Temperature.Max >= 60)
            {
                return "level_D";
            }
            else
            {
                return device.Temperature.Dif switch
                {
                    var value when value >= 0 && value < 10 => "level_A",
                    var value when value < 20 => "level_B",
                    var value when value < 40 => "level_C",
                    var value when value >= 40 => "level_D",
                    _ => "level_Z" 
                };
            }
        }

        public string absContext(float? tmp)
        {
            if (tmp.HasValue)
            {
                return Math.Round(Math.Abs((double)tmp!),2).ToString();
            }
            else{
                return "";
            }
        }

        public string SetIcon(float? tmp)
        {
            string icon = tmp switch
            {
                > 0 => "bi-caret-up-fill",
                < 0 => "bi-caret-down-fill",
                _ => string.Empty
            };
            return icon;
        }

        #endregion

        #region Public Methods - Modal Controll

        public async Task ShowModal(CurDevice device)
        {
            if (device != null)
            {
                modal!.SetModal(device!);
                await modal!.OnShowModal();
            }
        }

        #endregion
    }
}
