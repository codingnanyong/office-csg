using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.TrMontrgSrv.EF.Config.Dashboard
{
    public class CurDeviceConfig : IEntityTypeConfiguration<CurDeviceEntity>
    {
        public void Configure(EntityTypeBuilder<CurDeviceEntity> builder)
        {
            builder.HasNoKey().ToView("fact_device", "public");
            /* builder.ToTable("fact_device", "dashboard");
             builder.HasKey(x => x.DeviceId).HasName("fact_device_pk");

             builder.HasOne(x => x.Frame)
                    .WithOne(f => f.CurDevice)
                    .HasForeignKey<CurFrameEntity>(f => f.DeviceId);

             builder.HasMany(x => x.Roies)
                    .WithOne(r => r.CurDevice)
                    .HasForeignKey(r => r.DeviceId);

             builder.HasMany(x => x.Boxes)
                    .WithOne(b => b.CurDevice)
                    .HasForeignKey(b => b.DeviceId);*/
        }
    }
}
