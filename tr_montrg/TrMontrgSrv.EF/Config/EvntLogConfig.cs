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
    public class EvntLogConfig : IEntityTypeConfiguration<EvntLogEntity>
    {
        public void Configure(EntityTypeBuilder<EvntLogEntity> builder)
        {
            // EvntLog Entity
            builder.HasKey(el => el.Id)
                   .HasName("evnt_log_pkey");
            builder.Property(c => c.Id)
                   .ValueGeneratedOnAdd();
            builder.Property(f => f.UpdatedDt)
                   .HasDefaultValueSql("current_timestamp");
            builder.HasIndex(c => new { c.Ymd, c.Hms })
                   .HasDatabaseName("evnt_log_ymd_hms_idx");
            builder.HasIndex(c => c.EvntDt)
                   .HasDatabaseName("evnt_log_evnt_dt_idx");
            builder.HasOne(el => el.Device)
                   .WithMany(d => d.EvntLogs)
                   .HasForeignKey(el => el.DeviceId)
                   .HasConstraintName("evnt_log_device_id_fkey");
            builder.HasIndex(el => el.DeviceId)
                   .HasDatabaseName("evnt_log_device_id_idx");
        }
    }
}
