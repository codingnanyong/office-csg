namespace CSG.MI.TrMontrgSrv.Dashboard.Infrastructure
{
    public static class AppSettingProvider
    {
        #region DB Connect

        public static string DbConnectionString { get; set; } = string.Empty;

        #endregion

        #region Web API

        public static string WebApiHostUri { get; set; } = string.Empty;

        public static int WebApiVersion { get; set; }

        public static string FDWApiHostUri { get; set; } = string.Empty;

        public static float FDWApiVersion { get; set; }

        #endregion
    }
}