using AutoMapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using CSG.MI.FDW.EF.Configs.Department;
using CSG.MI.FDW.EF.Configs.Feedback;
using CSG.MI.FDW.EF.Configs.Production.DataMart;
using CSG.MI.FDW.EF.Configs.Production.PCC;
using CSG.MI.FDW.EF.Configs.Production.RTLS;
using CSG.MI.FDW.EF.Entities.Department;
using CSG.MI.FDW.EF.Entities.Feedback;
using CSG.MI.FDW.EF.Entities.Production.PCC;
using CSG.MI.FDW.EF.Entities.Production.RTLS;
using CSG.MI.FDW.EF.Entities.Production.DataMart;
using CSG.MI.FDW.EF.Configs.IoT;
using Entities.Analysis;
using Configs.Analysis;
using CSG.MI.FDW.EF.Data;

namespace Data;

public partial class HQDbContext : BaseDbContext
{
    #region Fields

    private static IConfigurationRoot? _configuration;

    #endregion

    #region Constructors

    public HQDbContext(DbContextOptions<HQDbContext> options) : base(options)
    {
    }

    #endregion

    #region DbSets

    #region hq-rtls DbSet

    public DbSet<TbErpPlanDataEntity> erpPlns { get; set; }

    public DbSet<TbEslLocationEntity> locations { get; set; }

    public DbSet<TbProcessLocEntity> processMsts { get; set; }

    #endregion

    #region hq-pcc DbSet

    // PCC - Master
    public DbSet<MstOpcdEntity> opMsts { get; set; }

    public DbSet<MstPlanIssuecdEntity> issueMsts { get; set; }

    public DbSet<MstMachineEntity> machineMsts { get; set; }

    public DbSet<MstComomEntity> commonMst {  get; set; }

    // PCC - Plan&Prod
    public DbSet<BomHeadEntity> boms { get; set; }

    public DbSet<PlanOpcdEntity> plns { get; set; }

    public DbSet<PlanStatusEntity> plnStatuses { get; set; }

    public DbSet<ProdMaterialEntity> prfMaterials { get; set; }

    public DbSet<ProdScanEntity> prfScans { get; set; }

    public DbSet<IssueHeadEntity> issueHeads { get; set; }

    public DbSet<IssueTailEntity> issueTails { get; set; }

    public DbSet<ReworkOpcdEntity> reworks { get; set; }

    public DbSet<ReworkHeadEntity> reworkHeads { get; set; }

    public DbSet<ReworkDetailEntity> reworkDetails { get; set; }

    public DbSet<MstUserEntity> users { get; set; }

    #endregion

    #region hq-services DbSet

    public DbSet<DailyStatusEntity> dailyStatus { get; set; }
    public DbSet<WsEntity> ws { get; set; }
    public DbSet<WsSummaryEntity> wsSummaries { get; set; }
    public DbSet<WsDetailEntity> wsDetails { get; set; }
    public DbSet<WsHistoryEntity> wsHistories { get; set; }
    public DbSet<WsCoodinateEntity> wsCoodinates { get; set; }
    public DbSet<WsCoordinateHistoryEntity> wsCoordinateHistories { get; set; }
    public DbSet<WsRateEntity> wsRate { get; set; }
    public DbSet<WsStatusEntity> wsStatus { get; set; }

    public DbSet<FactoryPlnTotalEntity> PlnTotal { get; set; }
    public DbSet<FactoryPrfTotalEntity> PrfTotal { get; set; }

    public DbSet<DevStylePredictionEntity> devPredictions { get; set; }

    #endregion

    #region hq-feedback DbSet

    public DbSet<CategoryEntity> categories { get; set; }

    public DbSet<FeedbackEntity> feedbacks { get; set; }

    public DbSet<SystemEntity> systems { get; set; }

    #endregion

    #region hq-dept DbSet

    public DbSet<DeptEntity> depts { get; set; }

    public DbSet<DeptRnREntity> deptRnRs { get; set; }

    #endregion

    #endregion

    #region OnConfiguring

    /// <summary>
    /// EF Framework to Auto DB First OnConfiguring
    /// Example: optionsBuilder.UseNpgsql("Host={DB_HOST};Database=hq;Username={DB_USER};Password={DB_PASSWORD}");
    /// </summary>

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
                .UseNpgsql(connectionString: _configuration["FDW:HQ:ConnectionString"], options => options.UseAdminDatabase("hq"))
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
        modelBuilder.HasDefaultSchema("hq");

        #region RTLS Configs

        modelBuilder.ApplyConfiguration(new ErpPlanConfig());
        modelBuilder.ApplyConfiguration(new EslLocationConfig());
        modelBuilder.ApplyConfiguration(new ProcessMstConfig());

        #endregion

        #region PCC Configs

        modelBuilder.ApplyConfiguration(new OpcdMstConfig());
        modelBuilder.ApplyConfiguration(new IssueMstConfig());
        modelBuilder.ApplyConfiguration(new MachineMstConfig());
        modelBuilder.ApplyConfiguration(new UserMstConfig());
        modelBuilder.ApplyConfiguration(new CommonMstConfig());

        modelBuilder.ApplyConfiguration(new BomHeadConfig());
        modelBuilder.ApplyConfiguration(new IssueHeadConfig());
        modelBuilder.ApplyConfiguration(new IssueTailConfig());
        modelBuilder.ApplyConfiguration(new PlanOpConfig());
        modelBuilder.ApplyConfiguration(new PlanStatusConfig());
        modelBuilder.ApplyConfiguration(new ProdMaterialConfig());
        modelBuilder.ApplyConfiguration(new ProdScanConfig());
        modelBuilder.ApplyConfiguration(new ReworkOpConfig());
        modelBuilder.ApplyConfiguration(new ReworkHeadConfig());
        modelBuilder.ApplyConfiguration(new ReworkDetailConfig());

        #endregion

        #region Production Configs

        modelBuilder.ApplyConfiguration(new DailyStatusConfig());
        modelBuilder.ApplyConfiguration(new WsConfig());
        modelBuilder.ApplyConfiguration(new WsSummaryConfig());
        modelBuilder.ApplyConfiguration(new WsHistoryConfig());
        modelBuilder.ApplyConfiguration(new WsDetailConfig());
        modelBuilder.ApplyConfiguration(new WsCoordinateConfig());
        modelBuilder.ApplyConfiguration(new WsCoordinateHistoryConfig());
        modelBuilder.ApplyConfiguration(new WsRateConfig());
        modelBuilder.ApplyConfiguration(new WsStatusConfig());

        modelBuilder.ApplyConfiguration(new FactoryPlnTotalConfig());
        modelBuilder.ApplyConfiguration(new FactoryPrfTotalConfig());

        #endregion

        #region Analysis Configs 

        modelBuilder.ApplyConfiguration(new DevStylePredictionConfig());

        #endregion

        #region Feedback Configs

        modelBuilder.ApplyConfiguration(new CategoryMstConfig());
        modelBuilder.ApplyConfiguration(new UserFeedbackConfig());
        modelBuilder.ApplyConfiguration(new SystemConfig());

        #endregion

        // TODO...
        #region Department Configs

        modelBuilder.ApplyConfiguration(new DeptConfig());
        modelBuilder.ApplyConfiguration(new DeptRnRConfig());

        #endregion

    }

    #endregion
}
