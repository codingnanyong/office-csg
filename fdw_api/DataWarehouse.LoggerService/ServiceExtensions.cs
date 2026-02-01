using Microsoft.Extensions.DependencyInjection;

namespace CSG.MI.FDW.LoggerService
{
	public static class ServiceExtensions
	{
		public static void ConfigureLoggerService(this IServiceCollection services) =>
			services.AddSingleton<ILoggerManager, LoggerManager>();
	}
}
