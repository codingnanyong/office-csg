using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Helpers;
using Microsoft.Extensions.Configuration;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc
{
    /// <summary>
    /// This class is to get values of AppSettings in appsettings.json file.
    /// </summary>
    public class AppSettings
    {
        #region Fields

        private static IConfigurationRoot _config;
        private readonly ConnStringCfg _connStringCfg;
        private readonly WatcherCfg _watcherCfg;

        #endregion

        #region Constructors

        public AppSettings()
        {
            _config = ConfigurationBuilderSingleton.ConfigurationRoot;
            _connStringCfg = new ConnStringCfg();
            _watcherCfg = new WatcherCfg();
        }

        #endregion

        #region Properties

        public ConnStringCfg ConnString => _connStringCfg;

        public WatcherCfg Watcher => _watcherCfg;

        #endregion

        #region Inner Classes

        /// <summary>
        ///
        /// </summary>
        public class ConnStringCfg
        {
            public static string TrMontrgSrv => _config["Data:TrMontrgSrv:ConnectionString"];

            public static string TrMontrgSrvIdentity => _config["Data:TrMontrgSrvIdentity:ConnectionString"];
        }

        /// <summary>
        ///
        /// </summary>
        public class WatcherCfg
        {
            public static string RootDir => _config["Watcher:Root"];

            public static string BackupDir => _config["Watcher:Backup"];

            public static int SubdirDepth => Convert.ToInt32(_config["Watcher:SubdirectoryDepth"]);

            public static string DirSearchPattern => _config["Watcher:DirSearchPattern"];

            public static string FileFilterPattern => _config["Watcher:FileFilterPattern"];

            public static int MaximumRetrials => Convert.ToInt32(_config["Watcher:MaximumRetrials"]);

            public static int RetryInterval => Convert.ToInt32(_config["Watcher:RetryInterval"]);

            public static bool ProcessOneByOne => Convert.ToBoolean(_config["Watcher:ProcessOneByOne"]);
        }

        #endregion
    }
}
