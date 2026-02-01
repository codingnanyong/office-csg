using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.TrMontrgSrv.EF.Config.Dashboard
{
    public class CurFrameConfig : IEntityTypeConfiguration<CurFrameEntity>
    {
        public void Configure(EntityTypeBuilder<CurFrameEntity> builder)
        {
            builder.HasNoKey().ToView("dim_cur_frame", "public");
            /*builder.ToTable("dim_cur_frame", "dashboard");

            builder.HasKey(x => new { x.DeviceId, x.Id }).HasName("dim_cur_frame_pk");*/
        }
    }
}
