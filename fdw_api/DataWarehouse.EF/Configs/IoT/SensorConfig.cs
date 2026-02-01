using CSG.MI.FDW.EF.Entities.IoT;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.IoT
{
    public class SensorConfig : IEntityTypeConfiguration<SensorEntity>
    {
        public void Configure(EntityTypeBuilder<SensorEntity> builder)
        {
            builder.HasNoKey().ToTable("sensor", "workshop");
        }
    }
}
