using CSG.MI.FDW.PerfAPIAnalyzer.Extensions;
using Microsoft.Extensions.Configuration;
using NBomber.CSharp;
using NBomber.Http;
using NBomber.Http.CSharp;
using Serilog;
using NBomber.Contracts;
using CSG.MI.FDW.PerfAPIAnalyzer.Extensions.Interface;

namespace CSG.MI.FDW.PerfAPIAnalyzer
{
    public class ProductionApiTest
    {
        public void Run()
        {
            ICustomConfiguration configProvider = new CustomConfiguration();
            var configuration = configProvider.LoadConfiguration();
            var productionEndpoints = configuration.GetSection("Productions").Get<string[]>();

            using var httpClient = new HttpClient();

            var scenarios = new List<ScenarioProps>();

            foreach (var endpoint in productionEndpoints!)
            {
                var scenario = Scenario.Create($"scenario_{endpoint}", async context =>
                {
                    var request = Http.CreateRequest("GET", endpoint)
                                      .WithHeader("Content-Type", "application/json");

                    var response = await Http.Send(httpClient, request);
                   
                    return response;
                })
                .WithoutWarmUp()
                .WithLoadSimulations(
                   Simulation.RampingConstant(copies: 50, during: TimeSpan.FromSeconds(30)),
                   Simulation.KeepConstant(copies: 50, during: TimeSpan.FromSeconds(30)),
                   Simulation.RampingConstant(copies: 0, during: TimeSpan.FromSeconds(30))
                );

                scenarios.Add(scenario);
            }

            NBomberRunner
               .RegisterScenarios(scenarios.ToArray())
               .WithWorkerPlugins(
                   new HttpMetricsPlugin(new[] { HttpVersion.Version1 })
               )
               .WithLoggerConfig(() => new LoggerConfiguration()
                   .MinimumLevel.Debug()
                   .WriteTo.File(
                        path: "logs\\NBomber_.log",
                        outputTemplate:"{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [ThreadId:{ThreadId}] {Message:lj}{NewLine}{Exception}", 
                        rollingInterval: RollingInterval.Day,
                        buffered: true))
               .Run();
        }  
    }
}
