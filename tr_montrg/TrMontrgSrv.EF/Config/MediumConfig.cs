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
    public class MediumConfig : IEntityTypeConfiguration<MediumEntity>
    {
        public void Configure(EntityTypeBuilder<MediumEntity> builder)
        {
            // Medium Entity
            builder.HasKey(m => new { m.Ymd, m.Hms, m.MediumType, m.DeviceId })
                   .HasName("medium_pkey");
            builder.Property(m => m.UpdatedDt)
                   .HasDefaultValueSql("current_timestamp");
            builder.HasOne(m => m.Device)
                   .WithMany(d => d.Media)
                   .HasForeignKey(m => new { m.DeviceId })
                   .HasConstraintName("medium_device_id_fkey");
            builder.HasIndex(m => m.DeviceId)
                   .HasDatabaseName("medium_device_id_idx");
        }
    }
}
