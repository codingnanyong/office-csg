using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class IssueMstConfig : IEntityTypeConfiguration<MstPlanIssuecdEntity>
	{
		public void Configure(EntityTypeBuilder<MstPlanIssuecdEntity> builder)
		{
			builder.HasKey(x => new { x.Factory, x.IssueCd }).HasName("pcc_mst_plan_issue_pkey");
		}
	}
}
