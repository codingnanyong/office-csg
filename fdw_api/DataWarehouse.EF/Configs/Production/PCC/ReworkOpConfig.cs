using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class ReworkOpConfig : IEntityTypeConfiguration<ReworkOpcdEntity>
	{
		public void Configure(EntityTypeBuilder<ReworkOpcdEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_plan_rework_opcd", "pcc");
		}
	}
}
