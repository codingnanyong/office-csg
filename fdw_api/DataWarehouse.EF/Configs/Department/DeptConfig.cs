using CSG.MI.FDW.EF.Entities.Department;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Department
{
    public class DeptConfig : IEntityTypeConfiguration<DeptEntity>
	{
		public void Configure(EntityTypeBuilder<DeptEntity> builder)
		{
			builder.HasKey(x => new { x.DeptId }).HasName("dept_mst_pkey");
			builder.Property(x => x.EditDate)
				   .HasDefaultValueSql("current_timestamp");
		}
	}
}
