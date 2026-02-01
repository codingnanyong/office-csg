using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class MachineMstConfig : IEntityTypeConfiguration<MstMachineEntity>
	{
		public void Configure(EntityTypeBuilder<MstMachineEntity> builder)
		{
			builder.HasNoKey().ToTable("pcc_machine_head", "pcc");
		}
	}
}
