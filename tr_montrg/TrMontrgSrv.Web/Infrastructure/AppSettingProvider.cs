using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Web.Infrastructure
{
    public static class AppSettingsProvider
    {
        public static string DbConnectionString { get; set; }

        #region Web API

        public static string WebApiHostUri { get; set; }

        public static int WebApiVersion { get; set; }

        #endregion

        #region Website

        public static string WebsiteHostUri { get; set; }

        #endregion

        public static bool ServiceActivateMontrgSvc { get; set; }

        public static string GoogleAnalyticsMeasurementId { get; set; }

        #region Email Account

        public static string EmailAccountSmtpHost { get; set; }

        public static int EmailAccountSmtpPort { get; set; }

        public static string EmailAccountId { get; set; }

        public static string EmailAccountPassword { get; set; }

        #endregion


    }
}
