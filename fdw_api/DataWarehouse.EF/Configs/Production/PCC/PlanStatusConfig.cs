using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class PlanStatusConfig : IEntityTypeConfiguration<PlanStatusEntity>
	{
		public void Configure(EntityTypeBuilder<PlanStatusEntity> builder)
		{
			builder.HasKey(x => new { x.Factory, x.WsNo }).HasName("pcc_plan_status_pkey");
		}
	}
}
