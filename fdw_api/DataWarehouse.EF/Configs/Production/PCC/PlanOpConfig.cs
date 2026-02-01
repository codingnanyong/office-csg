using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class PlanOpConfig : IEntityTypeConfiguration<PlanOpcdEntity>
	{
		public void Configure(EntityTypeBuilder<PlanOpcdEntity> builder)
		{
			builder.HasKey(x => new { x.Factory, x.WsNo, x.OpCd }).HasName("pcc_plan_opcd_pk");
		}
	}
}
