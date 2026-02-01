using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.RTLS
{
    public class ErpPlanConfig : IEntityTypeConfiguration<TbErpPlanDataEntity>
	{
		public void Configure(EntityTypeBuilder<TbErpPlanDataEntity> builder)
		{
			//builder.HasNoKey().ToTable("tb_erp_plan_data", "rtls");
			builder.HasKey(x => new { x.Id, x.UpdateDate })
			 .HasName("tb_erp_plan_data_pkey");

			builder.HasIndex(e => e.BeaconTagId, "tagid_idx");

			builder.HasIndex(e => e.Modelname, "model_idx");

			builder.HasIndex(e => e.PassType, "passtype_idx");

			builder.HasIndex(e => e.Pm, "pm_idx");

			builder.HasIndex(e => new { e.Pm, e.Modelname }, "pm_model_idx");

			builder.HasIndex(e => e.Season, "season_idx");

			builder.HasIndex(e => e.Wsno, "wsno_idx");

			builder.HasIndex(e => e.UpdateDate, "date_idx");
		}
	}
}
