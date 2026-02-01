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
    public class BoxCtrlConfig : IEntityTypeConfiguration<BoxCtrlEntity>
    {
        public void Configure(EntityTypeBuilder<BoxCtrlEntity> builder)
        {
            // BoxCtrl Entity
            builder.HasKey(bc => bc.DeviceId)
                   .HasName("box_ctrl_pkey");
            builder.HasOne(bc => bc.Device)
                   .WithOne(d => d.BoxCtrl)
                   .HasForeignKey<BoxCtrlEntity>(fc => fc.DeviceId)
                   .HasConstraintName("box_ctrl_device_id_fkey");
            builder.HasIndex(bc => bc.DeviceId)
                   .HasDatabaseName("box_ctrl_device_id_idx");
        }
    }
}
