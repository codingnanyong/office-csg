using System;
using CSG.MI.TrMontrgSrv.EF;
using CSG.MI.TrMontrgSrv.Helpers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace CSG.MI.TrMontrgSrv.DataMigrator
{
    class Program
    {
        static IConfigurationRoot _configuration;
        static DbContextOptionsBuilder<AppDbContext> _optionsBuilder;

        static void Main(string[] args)
        {
            if (args is null)
            {
                //throw new ArgumentNullException(nameof(args));
            }

            BuildOptions();
            EnsureAndRunMigrations();
            ExecuteCustomSeedData();
        }

        static void BuildOptions()
        {
            _configuration = ConfigurationBuilderSingleton.ConfigurationRoot;
            _optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
            _optionsBuilder.UseNpgsql(_configuration["Data:TrMontrgSrv:ConnectionString"]);
        }

        static void EnsureAndRunMigrations()
        {
            using var ctx = new AppDbContext(_optionsBuilder.Options);
            ctx.Database.EnsureCreated();
            ctx.Database.Migrate();
        }

        private static void ExecuteCustomSeedData()
        {
            //using var ctx = new AppDbContext(_optionsBuilder.Options);

        }
    }
}
