using AutoMapper;
using CSG.MI.FDW.EF.Configs.IoT;
using CSG.MI.FDW.EF.Data;
using CSG.MI.FDW.EF.Entities.IoT;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Data
{
    public class IoTDbContext : BaseDbContext
    {
        #region Fields

        private static IConfigurationRoot? _configuration;

        #endregion

        #region Constructors

        public IoTDbContext(DbContextOptions<IoTDbContext> options) : base(options)
        {
        }

        #endregion

        #region hq 3D PrinterRoom

        public DbSet<SensorEntity> sensors { get; set; }

        public DbSet<TemperatureEntity> temperature { get; set; }

        #endregion

        #region OnConfiguring

        /// <summary>
        /// DB connection settings
        /// </summary>
        /// <param name="optionsBuilder"></param>
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);

            optionsBuilder.UseLoggerFactory(AppLoggerFactory);

            if (optionsBuilder.IsConfigured == false)
            {
                var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
                var builder = new ConfigurationBuilder()
                                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                                .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true);
                _configuration = builder.Build();
                optionsBuilder
                    .UseLoggerFactory(AppLoggerFactory)
                    .ConfigureWarnings(x => x.Log((RelationalEventId.ConnectionOpened, LogLevel.Information), (RelationalEventId.ConnectionClosed, LogLevel.Information)))
                    .UseNpgsql(connectionString: _configuration["FDW:IoT:ConnectionString"], options => options.UseAdminDatabase("iot"))
                    .UseSnakeCaseNamingConvention();
            }
        }

        #endregion

        #region ModelConfigs

        /// <summary>
        /// Add configs to ModelConfig
        /// </summary>
        /// <param name="modelBuilder"></param>
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.HasDefaultSchema("3dprinter");

            #region IoT Configs

            modelBuilder.ApplyConfiguration(new SensorConfig());
            modelBuilder.ApplyConfiguration(new TemperatureConfig());

            #endregion

        }
        #endregion

    }
}
