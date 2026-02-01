using CSG.MI.FDW.EF.Entities.Production.PCC;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.FDW.EF.Configs.Production.PCC
{
    public class UserMstConfig : IEntityTypeConfiguration<MstUserEntity>
	{
		public void Configure(EntityTypeBuilder<MstUserEntity> builder)
		{
			builder.HasKey(x => new { x.Factory, x.Id }).HasName("pcc_user_info_pk");
		}
	}
}
