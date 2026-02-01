using CSG.MI.FDW.EF.Entities.IoT;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.IoT
{
    public class MachineConfig : IEntityTypeConfiguration<MachineEntity>
    {
        public void Configure(EntityTypeBuilder<MachineEntity> builder)
        {
            builder.HasNoKey().ToTable("machine", "iot");
        }
    }
}
