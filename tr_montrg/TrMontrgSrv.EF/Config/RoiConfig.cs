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
    public class RoiConfig : IEntityTypeConfiguration<RoiEntity>
    {
        public void Configure(EntityTypeBuilder<RoiEntity> builder)
        {
            // Roi Entity
            builder.HasKey(r => new { r.Ymd, r.Hms, r.RoiId, r.DeviceId })
                   .HasName("roi_pkey");
            builder.Property(r => r.UpdatedDt)
                   .HasDefaultValueSql("current_timestamp");
            builder.HasOne(r => r.Device)
                   .WithMany(d => d.Rois)
                   .HasForeignKey(r => new { r.DeviceId })
                   .HasConstraintName("roi_device_id_fkey");
            builder.HasIndex(r => r.DeviceId)
                   .HasDatabaseName("roi_device_id_idx");
            builder.HasIndex(r => new { r.DeviceId, r.RoiId, r.CaptureDt })
                   .HasDatabaseName("roi_device_id_roi_id_capture_dt_idx");
        }
    }
}
