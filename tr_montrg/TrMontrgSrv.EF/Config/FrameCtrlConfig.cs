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
    public class FrameCtrlConfig : IEntityTypeConfiguration<FrameCtrlEntity>
    {
        public void Configure(EntityTypeBuilder<FrameCtrlEntity> builder)
        {
            // FrameCtrl Entity
            builder.HasKey(fc => fc.DeviceId)
                   .HasName("frame_ctrl_pkey");
            builder.HasOne(fc => fc.Device)
                   .WithOne(d => d.FrameCtrl)
                   .HasForeignKey<FrameCtrlEntity>(fc => fc.DeviceId)
                   .HasConstraintName("frame_ctrl_device_id_fkey");
            builder.HasIndex(fc => fc.DeviceId)
                   .HasDatabaseName("frame_ctrl_device_id_idx");
        }
    }
}
