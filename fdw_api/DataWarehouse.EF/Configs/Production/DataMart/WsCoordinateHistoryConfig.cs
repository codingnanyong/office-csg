using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class WsCoordinateHistoryConfig : IEntityTypeConfiguration<WsCoordinateHistoryEntity>
    {
        public void Configure(EntityTypeBuilder<WsCoordinateHistoryEntity> builder)
        {
            builder.HasNoKey().ToTable("worksheets_coordinate_history", "services");
        }
    }
}