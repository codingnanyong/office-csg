using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc
{
    public static class App
    {
        #region Constructors

        static App()
        {
            WorkingPath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            Settings = new AppSettings();
        }

        #endregion

        #region Properties

        public static string AppName => "TrDataImporterSvc";

        public static string ServiceName => "TR Data Importer Service";

        public static string ServiceSourceName => "TR Data Importer Service Source";

        public static string WorkingPath
        {
            get;
            private set;
        }

        public static AppSettings Settings
        {
            get;
            private set;
        }

        #endregion
    }
}
