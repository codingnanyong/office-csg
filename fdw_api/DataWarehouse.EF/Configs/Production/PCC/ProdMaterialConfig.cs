using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class ProdMaterialConfig : IEntityTypeConfiguration<ProdMaterialEntity>
	{
		public void Configure(EntityTypeBuilder<ProdMaterialEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_prod_material", "pcc");
		}
	}
}
