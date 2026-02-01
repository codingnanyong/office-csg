using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_mst_user", Schema = "pcc")]
    public class MstUserEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varchar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("user_id", TypeName = "varchar(50)")]
        public string Id { get; set; } = String.Empty;

        [Column("user_name", TypeName = "varchar(100)")]
        public string Name { get; set; } = String.Empty;
    }
}
