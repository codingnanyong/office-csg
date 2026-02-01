using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class WsDetailConfig : IEntityTypeConfiguration<WsDetailEntity>
	{
		public void Configure(EntityTypeBuilder<WsDetailEntity> builder)
		{
			builder.HasNoKey().ToTable("worksheets_detail", "services");

            builder.HasIndex(e => e.Category, "idx_dtl_category");
            builder.HasIndex(e => e.ColorVer, "idx_dtl_color");
            builder.HasIndex(e => e.DevColorwayId, "idx_dtl_colorway");
            builder.HasIndex(e => e.DevName, "idx_dtl_devname");
            builder.HasIndex(e => e.DevStyleNumber, "idx_dtl_devstyle_num");
            builder.HasIndex(e => e.ModelId, "idx_dtl_model");
            builder.HasIndex(e => e.PM, "idx_dtl_pm");
            builder.HasIndex(e => e.PmName, "idx_dtl_pm_kor");
            builder.HasIndex(e => e.SampleEts, "idx_dtl_sample_ets");
            builder.HasIndex(e => e.Season, "idx_dtl_season");
            builder.HasIndex(e => e.StCd, "idx_dtl_st");
            builder.HasIndex(e => e.SubStCd, "idx_dtl_st_sub");
            builder.HasIndex(e => e.StyleCd, "idx_dtl_style");
            builder.HasIndex(e => e.WsNo, "idx_dtl_ws");
        }
	}
}
