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
    public class DeviceConfig : IEntityTypeConfiguration<DeviceEntity>
    {
        public void Configure(EntityTypeBuilder<DeviceEntity> builder)
        {
            // Device Entity
            builder.HasKey(d => new { d.DeviceId })
                   .HasName("device_pkey");
            builder.HasIndex(d => new { d.LocationId, d.PlantId })
                   .HasDatabaseName("device_loc_id_plant_id_idx");
            builder.Property(d => d.UpdatedDt)
                   .HasDefaultValueSql("current_timestamp");
            builder.HasMany(d => d.Frames)
                   .WithOne(f => f.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasMany(d => d.Rois)
                   .WithOne(r => r.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasMany(d => d.Boxes)
                   .WithOne(b => b.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasMany(d => d.Cfgs)
                   .WithOne(c => c.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasOne(d => d.DeviceCtrl)
                   .WithOne(dc => dc.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasOne(d => d.FrameCtrl)
                   .WithOne(fc => fc.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasMany(d => d.RoiCtrls)
                   .WithOne(rc => rc.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasOne(d => d.BoxCtrl)
                   .WithOne(bc => bc.Device)
                   .OnDelete(DeleteBehavior.Cascade);
            builder.HasMany(d => d.EvntLogs)
                   .WithOne(el => el.Device)
                   .OnDelete(DeleteBehavior.Cascade);
        }
    }
}
