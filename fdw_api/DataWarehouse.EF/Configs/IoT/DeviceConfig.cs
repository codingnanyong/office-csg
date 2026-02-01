using CSG.MI.FDW.EF.Entities.IoT;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.IoT
{
    public class DeviceConfig : IEntityTypeConfiguration<DeviceEntity>
    {
        public void Configure(EntityTypeBuilder<DeviceEntity> builder)
        {
            builder.HasNoKey().ToTable("device", "iot");
        }
    }
}
