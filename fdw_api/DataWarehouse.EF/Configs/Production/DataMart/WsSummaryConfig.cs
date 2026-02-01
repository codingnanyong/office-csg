using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class WsSummaryConfig : IEntityTypeConfiguration<WsSummaryEntity>
	{
		public void Configure(EntityTypeBuilder<WsSummaryEntity> builder)
		{
			builder.HasNoKey().ToTable("worksheets_summary", "services"); ;

			builder.HasIndex(e => e.OpCd, "idx_smr_op");

			builder.HasIndex(e => new { e.Factory, e.OpCd }, "idx_smr_fac_op");
		}
	}
}
