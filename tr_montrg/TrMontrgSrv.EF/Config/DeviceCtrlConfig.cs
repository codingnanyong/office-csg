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
    public class DeviceCtrlConfig : IEntityTypeConfiguration<DeviceCtrlEntity>
    {
        public void Configure(EntityTypeBuilder<DeviceCtrlEntity> builder)
        {
            // DeviceCtrl Entity
            builder.HasKey(dc => dc.DeviceId)
                   .HasName("device_ctrl_pkey");
            builder.HasOne(dc => dc.Device)
                   .WithOne(d => d.DeviceCtrl)
                   .HasForeignKey<DeviceCtrlEntity>(dc => dc.DeviceId)
                   .HasConstraintName("device_ctrl_device_id_fkey");
            builder.HasIndex(dc => dc.DeviceId)
                   .HasDatabaseName("device_ctrl_device_id_idx");
        }
    }
}
