using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class WsStatusConfig : IEntityTypeConfiguration<WsStatusEntity>
    {
        public void Configure(EntityTypeBuilder<WsStatusEntity> builder)
        {
            builder.HasNoKey().ToTable("worksheets_status", "services");
        }
    }
}
