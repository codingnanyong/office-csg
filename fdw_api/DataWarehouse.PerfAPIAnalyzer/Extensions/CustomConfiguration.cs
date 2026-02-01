using CSG.MI.FDW.PerfAPIAnalyzer.Extensions.Interface;
using Microsoft.Extensions.Configuration;

namespace CSG.MI.FDW.PerfAPIAnalyzer.Extensions
{
    public class CustomConfiguration : ICustomConfiguration
    {
        public IConfiguration LoadConfiguration()
        {
            var basePath = AppContext.BaseDirectory;
            var configPath = Path.Combine(basePath, "appsettings.json");

            Console.WriteLine($"Base Directory: {basePath}");
            Console.WriteLine($"Config Path: {configPath}");

            if (!File.Exists(configPath))
            {
                throw new FileNotFoundException($"The configuration file 'appsettings.json' was not found at {configPath}");
            }

            var builder = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile(configPath, optional: false, reloadOnChange: true);

            return builder.Build();
        }
    }
}