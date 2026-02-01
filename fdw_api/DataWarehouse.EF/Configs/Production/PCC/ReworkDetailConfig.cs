using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class ReworkDetailConfig : IEntityTypeConfiguration<ReworkDetailEntity>
	{
		public void Configure(EntityTypeBuilder<ReworkDetailEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_plan_rework_detail", "pcc");
		}
	}
}
