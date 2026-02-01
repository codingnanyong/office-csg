using Microsoft.AspNetCore.Components;
using System.Globalization;

namespace CSG.MI.TrMontrgSrv.Dashboard.Components.Layout
{
    public partial class CultureSelector
    {
        [Inject]
        public NavigationManager? NavigationManager { get; set; }

        private void OnSelectedCultureChanged(string culture)
        {
            var uri = new Uri(NavigationManager!.Uri).GetComponents(UriComponents.PathAndQuery, UriFormat.Unescaped);
            var cultureEscaped = Uri.EscapeDataString(culture);
            var uriEscaped = Uri.EscapeDataString(uri);

            NavigationManager.NavigateTo($"Culture/Set?culture={cultureEscaped}&redirectUri={uriEscaped}", forceLoad: true);
        }
    }
}
