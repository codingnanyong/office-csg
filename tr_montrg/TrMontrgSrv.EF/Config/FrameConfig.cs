using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.TrMontrgSrv.EF.Config
{
    public class FrameConfig : IEntityTypeConfiguration<FrameEntity>
    {
        public void Configure(EntityTypeBuilder<FrameEntity> builder)
        {
            // Frame Entity
            builder.HasKey(f => new { f.Ymd, f.Hms, f.DeviceId })
                   .HasName("frame_pkey");
            builder.Property(f => f.UpdatedDt)
                   .HasDefaultValueSql("current_timestamp");
            builder.HasOne(f => f.Device)
                   .WithMany(d => d.Frames)
                   .HasForeignKey(f => new { f.DeviceId })
                   .HasConstraintName("frame_device_id_fkey");
            builder.HasIndex(f => f.DeviceId)
                   .HasDatabaseName("frame_device_id_idx");
            builder.HasIndex(f => new { f.DeviceId, f.CaptureDt })
                   .HasDatabaseName("frame_device_id_capture_dt_idx");
        }
    }
}
