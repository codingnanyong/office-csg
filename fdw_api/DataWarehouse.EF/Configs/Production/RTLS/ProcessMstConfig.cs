using CSG.MI.FDW.EF.Entities.Production.RTLS;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.RTLS
{
    public class ProcessMstConfig : IEntityTypeConfiguration<TbProcessLocEntity>
	{
		public void Configure(EntityTypeBuilder<TbProcessLocEntity> builder)
		{
			builder.HasNoKey().ToTable("tb_process_loc", "rtls");
		}
	}
}
