using CSG.MI.TrMontrgSrv.EF.Entities.Dashboard;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.TrMontrgSrv.EF.Config.Dashboard
{
    public class CurBoxConfig : IEntityTypeConfiguration<CurBoxEntity>
    {
        public void Configure(EntityTypeBuilder<CurBoxEntity> builder)
        {
            builder.HasNoKey().ToView("dim_cur_box", "public");
            /*builder.ToTable("dim_cur_box", "dashboard");

            builder.HasKey(x => new { x.DeviceId, x.Id }).HasName("dim_cur_box_pk");*/
        }
    }
}