using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class BomHeadConfig : IEntityTypeConfiguration<BomHeadEntity>
	{
		public void Configure(EntityTypeBuilder<BomHeadEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_plan_bom_head", "pcc");

			builder.HasIndex(e => new { e.Factory, e.WsNo }, "bom_idx");
		}
	}
}
