using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class FactoryPlnTotalConfig : IEntityTypeConfiguration<FactoryPlnTotalEntity>
	{
		public void Configure(EntityTypeBuilder<FactoryPlnTotalEntity> builder)
		{
			builder.HasNoKey().ToView("pln_tot", "services");
		}
	}
}
