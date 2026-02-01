using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class WsConfig : IEntityTypeConfiguration<WsEntity>
    {
        public void Configure(EntityTypeBuilder<WsEntity> builder)
        {
            builder.HasNoKey().ToTable("worksheets", "services");
        }
    }
}
