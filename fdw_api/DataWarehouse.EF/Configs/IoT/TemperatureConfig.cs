using CSG.MI.FDW.EF.Entities.IoT;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.IoT
{
    public class TemperatureConfig : IEntityTypeConfiguration<TemperatureEntity>
    {
        public void Configure(EntityTypeBuilder<TemperatureEntity> builder)
        {
            builder.HasNoKey().ToTable("temperature", "workshop");

            builder.HasIndex(e => e.CaptureDate, "temperature_capture_dt_idx");

            builder.HasIndex(e => new { e.SensorId,e.CaptureDate }, "temperature_sensor_id_capture_dt_idx");
        }
    }
}
