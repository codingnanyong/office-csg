using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    internal class WsRateConfig : IEntityTypeConfiguration<WsRateEntity>
    {
        public void Configure(EntityTypeBuilder<WsRateEntity> builder)
        {
            builder.HasNoKey().ToView("worksheets_rate", "services");
        }
    }
}
