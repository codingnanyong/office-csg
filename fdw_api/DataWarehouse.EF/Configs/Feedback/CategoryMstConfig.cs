using CSG.MI.FDW.EF.Entities.Feedback;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Feedback
{
	public class CategoryMstConfig : IEntityTypeConfiguration<CategoryEntity>
	{
		public void Configure(EntityTypeBuilder<CategoryEntity> builder)
		{
			builder.HasKey(x => x.Key).HasName("PK_category");
			builder.Property(x => x.EditDate)
				   .HasDefaultValueSql("current_timestamp");
		}
	}
}
