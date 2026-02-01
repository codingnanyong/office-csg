using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_mst_com",Schema ="pcc")]
    public class MstComomEntity : BaseEntity
    {
        [Column("factory", TypeName = "varchar(10)")]
        public string Factory { get; set; } = String.Empty;

        [Column("com_div", TypeName = "varchar(30)")]
        public string ComDiv { get; set; } = String.Empty;

        [Column("com_cd",TypeName ="varchar(100)")]
        public string ComCode { get; set; } = String.Empty;

        [Column("com_name", TypeName = "varchar(100)")]
        public string ComName { get; set; } = String.Empty;
    }
}
