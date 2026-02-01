using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class DailyStatusConfig : IEntityTypeConfiguration<DailyStatusEntity>
	{
		public void Configure(EntityTypeBuilder<DailyStatusEntity> builder)
		{
			builder.HasNoKey().ToTable("dailystatus", "services");
		}
	}
}
