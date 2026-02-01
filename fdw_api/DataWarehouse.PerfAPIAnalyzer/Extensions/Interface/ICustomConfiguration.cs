using Microsoft.Extensions.Configuration;

namespace CSG.MI.FDW.PerfAPIAnalyzer.Extensions.Interface
{
    public interface ICustomConfiguration
    {
        IConfiguration LoadConfiguration();
    }
}
