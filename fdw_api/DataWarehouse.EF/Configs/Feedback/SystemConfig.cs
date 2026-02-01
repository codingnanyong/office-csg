using CSG.MI.FDW.EF.Entities.Feedback;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.FDW.EF.Configs.Feedback
{
    public class SystemConfig : IEntityTypeConfiguration<SystemEntity>
    {
        public void Configure(EntityTypeBuilder<SystemEntity> builder)
        {
            builder.HasKey(x => x.Id).HasName("PK_system");
        }
    }
}
