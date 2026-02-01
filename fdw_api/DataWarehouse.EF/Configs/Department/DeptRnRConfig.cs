using CSG.MI.FDW.EF.Entities.Department;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Department
{
    public class DeptRnRConfig : IEntityTypeConfiguration<DeptRnREntity>
	{
		public void Configure(EntityTypeBuilder<DeptRnREntity> builder)
		{
			builder.HasKey(x => new { x.DeptId }).HasName("dept_rnr_pkey");
			builder.Property(x => x.EditDate)
				   .HasDefaultValueSql("current_timestamp");
			builder.HasOne(x => x.DeptNavigation)
				   .WithMany()
				   .HasForeignKey(x => x.DeptId)
				   .HasPrincipalKey(x => x.DeptId)
				   .HasConstraintName("dept_rnr_fkey")
				   .OnDelete(DeleteBehavior.SetNull);
		}
	}
}
