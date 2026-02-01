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
    public class CfgConfig : IEntityTypeConfiguration<CfgEntity>
    {
        public void Configure(EntityTypeBuilder<CfgEntity> builder)
        {
            // Cfg Entity
            builder.HasKey(c => c.Id)
                   .HasName("cfg_pkey");
            builder.Property(c => c.Id)
                   .ValueGeneratedOnAdd();
            builder.HasIndex(c => new { c.Ymd, c.Hms })
                   .HasDatabaseName("cfg_ymd_hms_idx");
            builder.Property(c => c.UpdatedDt)
                   .HasDefaultValueSql("current_timestamp");
            builder.HasOne(c => c.Device)
                   .WithMany(d => d.Cfgs)
                   .HasForeignKey(c => new { c.DeviceId })
                   .HasConstraintName("cfg_device_id_fkey");
            builder.HasIndex(c => c.DeviceId)
                   .HasDatabaseName("cfg_device_id_idx");
        }
    }
}
