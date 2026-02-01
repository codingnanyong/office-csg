using AutoMapper;
using CSG.MI.TrMontrgSrv.EF.Config;
using CSG.MI.TrMontrgSrv.EF.Config.Dashboard;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.EF
{
    public class AppDbContext : DbContext
    {
        #region Fields

        private static IConfigurationRoot _configuration;
        public static readonly ILoggerFactory AppLoggerFactory = LoggerFactory.Create(builder => { builder.AddConsole(); });

        #endregion

        #region Constructors

        public AppDbContext(DbContextOptions<AppDbContext> options)
            : base(options)
        {
            var config = new MapperConfiguration(cfg =>
            {
                cfg.AddProfile<EntityMappingProfile>();
                cfg.AddProfile<ModelMappingProfile>();
            });

            Mapper = config.CreateMapper();
        }

        #endregion

        #region Properties

        public DbSet<DeviceEntity> DeviceDbSet { get; set; }

        public DbSet<FrameEntity> FrameDbSet { get; set; }

        public DbSet<RoiEntity> RoiDbSet { get; set; }

        public DbSet<BoxEntity> BoxDbSet { get; set; }

        public DbSet<CfgEntity> CfgDbSet { get; set; }

        public DbSet<MediumEntity> MediumDbSet { get; set; }

        public DbSet<DeviceCtrlEntity> DeviceCtrlDbSet { get; set; }

        public DbSet<FrameCtrlEntity> FrameCtrlDbSet { get; set; }

        public DbSet<RoiCtrlEntity> RoiCtrlDbSet { get; set; }

        public DbSet<BoxCtrlEntity> BoxCtrlDbSet { get; set; }

        public DbSet<EvntLogEntity> EvntLogDbSet { get; set; }

        public DbSet<GrpKeyEntity> GrpKeyDbSet { get; set; }

        public IMapper Mapper { get; private set; }

        #region Temperature Dashboard

        public DbSet<CurDeviceEntity> curDeviceSet { get; set; }

        public DbSet<CurFrameEntity> curFrameSet { get; set; }

        public DbSet<CurBoxEntity> curBoxSet { get; set; }

        public DbSet<CurRoiEntity> curRoiSet { get; set; }

        #endregion

        public static string UserProvider
        {
            get
            {
                //if (!String.IsNullOrEmpty(WindowsIdentity.GetCurrent().Name))
                //    return WindowsIdentity.GetCurrent().Name.Split('\\')[1];

                return String.Empty;
            }
        }

        public Func<DateTime> TimestampProvider { get; set; } = () => DateTime.UtcNow;

        #endregion

        #region Protected Methods

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            base.OnConfiguring(optionsBuilder);

            if (optionsBuilder.IsConfigured == false)
            {
                var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
                var builder = new ConfigurationBuilder()
                                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                                .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true);
                _configuration = builder.Build();
                optionsBuilder
                    .UseLoggerFactory(AppLoggerFactory)
                    .ConfigureWarnings(b => b.Log((RelationalEventId.ConnectionOpened, LogLevel.Information),
                                                  (RelationalEventId.ConnectionClosed, LogLevel.Information)))
                    .UseNpgsql(connectionString: _configuration["Data:TrMontrgSrv:ConnectionString"],
                               options => options.UseAdminDatabase("postgres"))
                    .UseSnakeCaseNamingConvention();
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.HasDefaultSchema("public");
            modelBuilder.UseTablespace("mi_default");

            modelBuilder.ApplyConfiguration(new DeviceConfig());
            modelBuilder.ApplyConfiguration(new FrameConfig());
            modelBuilder.ApplyConfiguration(new RoiConfig());
            modelBuilder.ApplyConfiguration(new BoxConfig());
            modelBuilder.ApplyConfiguration(new CfgConfig());
            modelBuilder.ApplyConfiguration(new MediumConfig());
            modelBuilder.ApplyConfiguration(new DeviceCtrlConfig());
            modelBuilder.ApplyConfiguration(new FrameCtrlConfig());
            modelBuilder.ApplyConfiguration(new RoiCtrlConfig());
            modelBuilder.ApplyConfiguration(new BoxCtrlConfig());
            modelBuilder.ApplyConfiguration(new EvntLogConfig());
            modelBuilder.ApplyConfiguration(new MailingAddrConfig());
            modelBuilder.ApplyConfiguration(new GrpKeyConfig());

            #region  temperature dashboard 

            modelBuilder.ApplyConfiguration(new CurDeviceConfig());
            modelBuilder.ApplyConfiguration(new CurFrameConfig());
            modelBuilder.ApplyConfiguration(new CurBoxConfig());
            modelBuilder.ApplyConfiguration(new CurRoiConfig());

            #endregion

        }

        #endregion

        #region Public Methods

        public virtual void Save()
        {
            base.SaveChanges();
        }

        public override int SaveChanges()
        {
            TrackChanges();
            return base.SaveChanges();
        }

        public override async Task<int> SaveChangesAsync(CancellationToken cancellationToken = default)
        {
            TrackChanges();
            return await base.SaveChangesAsync(cancellationToken);
        }

        #endregion

        #region Private Methods

        private void TrackChanges()
        {
            foreach (var entry in this.ChangeTracker.Entries().Where(e => e.State == EntityState.Added ||
                                                                          e.State == EntityState.Modified))
            {
                if (entry.Entity is IAuditable)
                {
                    var auditable = entry.Entity as IAuditable;
                    if (entry.State == EntityState.Added)
                    {
                        auditable.CreatedBy = UserProvider;
                        auditable.CreatedOn = TimestampProvider();
                        auditable.UpdatedOn = TimestampProvider();
                    }
                    else
                    {
                        auditable.UpdatedBy = UserProvider;
                        auditable.UpdatedOn = TimestampProvider();
                    }
                }
            }
        }

        #endregion
    }
}
