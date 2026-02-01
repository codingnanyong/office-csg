using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class OpcdMstConfig : IEntityTypeConfiguration<MstOpcdEntity>
	{
		public void Configure(EntityTypeBuilder<MstOpcdEntity> builder)
		{
			builder.HasKey(x => new { x.Factory, x.OpCd }).HasName("pcc_mst_opcd_pkey");

			builder.HasIndex(e => new { e.Factory, e.OpCd }, "pcc_mst_opcd_idx");
		}
	}
}
