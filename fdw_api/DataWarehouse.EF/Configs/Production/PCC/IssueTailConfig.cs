using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class IssueTailConfig : IEntityTypeConfiguration<IssueTailEntity>
	{
		public void Configure(EntityTypeBuilder<IssueTailEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_plan_issue_tail", "pcc");
		}
	}
}
