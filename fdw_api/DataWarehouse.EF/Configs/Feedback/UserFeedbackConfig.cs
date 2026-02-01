using CSG.MI.FDW.EF.Entities.Feedback;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Feedback
{
	public class UserFeedbackConfig : IEntityTypeConfiguration<FeedbackEntity>
	{
		public void Configure(EntityTypeBuilder<FeedbackEntity> builder)
		{
            builder.HasKey(x => x.Seq).HasName("PK_feedback");

            builder.Property(x => x.EditDate).HasDefaultValueSql("current_timestamp");

            builder.HasIndex(x => x.EditDate).HasDatabaseName("IX_feedback_edit_date");

            builder.HasOne(x => x.SystemNavigation)
                   .WithMany()
                   .HasForeignKey(x => x.System)
                   .HasPrincipalKey(x => x.Id)
                   .HasConstraintName("FK_feedback_system")
                   .OnDelete(DeleteBehavior.SetNull);

            builder.HasOne(x => x.CategoryNavigation)
                   .WithMany()
                   .HasForeignKey(x => x.Category)
                   .HasPrincipalKey(x => x.Key)
                   .HasConstraintName("FK_feedback_category")
                   .OnDelete(DeleteBehavior.SetNull);
        }
	}
}