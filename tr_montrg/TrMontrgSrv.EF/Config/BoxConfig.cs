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
    public class BoxConfig : IEntityTypeConfiguration<BoxEntity>
    {
        public void Configure(EntityTypeBuilder<BoxEntity> builder)
        {
            // Box Entity
            builder.HasKey(b => new { b.Ymd, b.Hms, b.BoxId, b.DeviceId })
                   .HasName("box_pkey");
            builder.Property(b => b.UpdatedDt)
                   .HasDefaultValueSql("current_timestamp");
            builder.HasOne(b => b.Device)
                   .WithMany(d => d.Boxes)
                   .HasForeignKey(b => new { b.DeviceId })
                   .HasConstraintName("box_device_id_fkey");
            builder.HasIndex(b => b.DeviceId)
                   .HasDatabaseName("box_device_id_idx");
        }
    }
}
