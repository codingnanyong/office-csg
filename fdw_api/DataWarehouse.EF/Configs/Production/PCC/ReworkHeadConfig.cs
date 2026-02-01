using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class ReworkHeadConfig : IEntityTypeConfiguration<ReworkHeadEntity>
	{
		public void Configure(EntityTypeBuilder<ReworkHeadEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_plan_rework_head", "pcc");
		}
	}
}
