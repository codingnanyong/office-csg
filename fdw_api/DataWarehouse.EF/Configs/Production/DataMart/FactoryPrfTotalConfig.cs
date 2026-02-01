using CSG.MI.FDW.EF.Entities.Production.DataMart;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.DataMart
{
    public class FactoryPrfTotalConfig : IEntityTypeConfiguration<FactoryPrfTotalEntity>
	{
		public void Configure(EntityTypeBuilder<FactoryPrfTotalEntity> builder)
		{
			builder.HasNoKey().ToView("prf_tot", "services");
		}
	}
}
