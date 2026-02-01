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
    public class RoiCtrlConfig : IEntityTypeConfiguration<RoiCtrlEntity>
    {
        public void Configure(EntityTypeBuilder<RoiCtrlEntity> builder)
        {
            // RoiCtrl Entity
            builder.HasKey(rc => new { rc.DeviceId, rc.RoiId })
                   .HasName("roi_ctrl_pkey");
            builder.HasOne(rc => rc.Device)
                   .WithMany(d => d.RoiCtrls)
                   .HasForeignKey(rc => rc.DeviceId)
                   .HasConstraintName("roi_ctrl_device_id_fkey");
            builder.HasIndex(rc => rc.DeviceId)
                   .HasDatabaseName("roi_ctrl_device_id_idx");
        }
    }
}
