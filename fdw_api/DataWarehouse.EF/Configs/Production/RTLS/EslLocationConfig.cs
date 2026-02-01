using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.RTLS
{
    public class EslLocationConfig : IEntityTypeConfiguration<TbEslLocationEntity>
	{
		public void Configure(EntityTypeBuilder<TbEslLocationEntity> builder)
		{
			builder.HasNoKey().ToTable("tb_esl_location", "rtls");
		}
	}
}