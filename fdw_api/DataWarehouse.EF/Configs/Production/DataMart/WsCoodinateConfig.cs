using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class WsCoodinateConfig : IEntityTypeConfiguration<WsCoodinateEntity>
    {
        public void Configure(EntityTypeBuilder<WsCoodinateEntity> builder)
        {
            builder.HasNoKey().ToTable("worksheets_coordinate", "services");
        }
    }
}
