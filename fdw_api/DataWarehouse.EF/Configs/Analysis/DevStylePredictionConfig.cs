using Entities.Analysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Configs.Analysis
{
    public class DevStylePredictionConfig : IEntityTypeConfiguration<DevStylePredictionEntity>
    {
        public void Configure(EntityTypeBuilder<DevStylePredictionEntity> builder)
        {
            builder.HasNoKey().ToTable("prediction_devstyle", "services");
        }
    }
}
