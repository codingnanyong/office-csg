using Microsoft.Extensions.DependencyInjection;

namespace CSG.MI.TrMontrgSrv.LoggerService
{
	public static class ServiceExtensions
	{
		public static void ConfigureLoggerService(this IServiceCollection services) =>
			services.AddScoped<ILoggerManager, LoggerManager>();
	}
}