using System;
using System.IO;
using System.Reflection;
using Microsoft.Extensions.Configuration;

namespace CSG.MI.TrMontrgSrv.Helpers
{
    public sealed class ConfigurationBuilderSingleton
    {
        #region Fields

        private static ConfigurationBuilderSingleton _instance = null;
        private static readonly object instanceLock = new();

        private static IConfigurationRoot _configruation;

        #endregion

        #region Constructors
        private ConfigurationBuilderSingleton()
        {
//#if DEBUG
//            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Debug");
//#endif
            var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            var builder = new ConfigurationBuilder()
                                .SetBasePath(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location))
                                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                                .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true)
                                .AddEnvironmentVariables();
            _configruation = builder.Build();
        }

        #endregion

        #region Properties

        public static ConfigurationBuilderSingleton Instance
        {
            get
            {
                lock (instanceLock)
                {
                    if (_instance == null)
                        _instance = new ConfigurationBuilderSingleton();

                    return _instance;
                }
            }
        }

        public static IConfigurationRoot ConfigurationRoot
        {
            get
            {
                if (_configruation == null)
                {
#pragma warning disable IDE0059 // Unnecessary assignment of a value
                    var x = ConfigurationBuilderSingleton.Instance;
#pragma warning restore IDE0059 // Unnecessary assignment of a value
                }

                return _configruation;
            }
        }


        #endregion
    }
}
