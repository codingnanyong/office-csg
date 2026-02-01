using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class CommonMstConfig : IEntityTypeConfiguration<MstComomEntity>
    {
        public void Configure(EntityTypeBuilder<MstComomEntity> builder)
        {
            builder.HasNoKey().ToTable("pcc_mst_com", "pcc");

            builder.HasIndex(e => e.ComDiv, "idx_comdiv");

            builder.HasIndex(e => new { e.ComDiv,e.ComCode }, "idx_div_cd");

            builder.HasIndex(e => new { e.Factory, e.ComDiv, e.ComCode }, "idx_fc_div_cd");
        }
    }
}
