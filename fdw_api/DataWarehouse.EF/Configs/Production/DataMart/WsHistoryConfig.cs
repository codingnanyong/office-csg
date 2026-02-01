using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class WsHistoryConfig : IEntityTypeConfiguration<WsHistoryEntity>
	{
		public void Configure(EntityTypeBuilder<WsHistoryEntity> builder)
		{
			builder.HasNoKey().ToTable("worksheets_history", "services");
		}
	}
}
