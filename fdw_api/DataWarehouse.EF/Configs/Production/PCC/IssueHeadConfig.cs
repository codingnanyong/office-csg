using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class IssueHeadConfig : IEntityTypeConfiguration<IssueHeadEntity>
	{
		public void Configure(EntityTypeBuilder<IssueHeadEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_plan_issue_head", "pcc");
		}
	}
}
