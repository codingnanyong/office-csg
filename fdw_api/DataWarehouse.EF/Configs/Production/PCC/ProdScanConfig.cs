using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class ProdScanConfig : IEntityTypeConfiguration<ProdScanEntity>
	{
		public void Configure(EntityTypeBuilder<ProdScanEntity> builder)
		{
			builder.HasKey(x => new { x.Factory, x.WsNo, x.OpCd, x.ProdSeq }).HasName("pcc_prod_scan_pk");

			builder.HasIndex(e => new { e.Factory, e.WsNo, e.OpCd, e.ProdSeq, e.Status }, "pcc_prod_scan_idx");

			builder.HasIndex(e => new { e.Factory, e.WsNo }, "pcc_prod_group_idx");
		}
	}
}
