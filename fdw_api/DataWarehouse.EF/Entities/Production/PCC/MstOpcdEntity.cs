using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_mst_opcd", Schema = "pcc")]
    public class MstOpcdEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varchar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Required, Column("op_cd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;

        [Column("op_name", TypeName = "varchar(500)")]
        public string Name { get; set; } = String.Empty;

        [Column("op_local_name", TypeName = "varchar(500)")]
        public string LocalName { get; set; } = String.Empty;

        [Column("op_leadtime", TypeName = "numeric")]
        public decimal LeadTime { get; set; } = Decimal.Zero;

        [Column("sort_no", TypeName = "numeric")]
        public decimal SortNo { get; set; } = Decimal.Zero;

        [Column("status", TypeName = "varchar(1)")]
        public string Status { get; set; } = "Y";
        
        [Column("op_capacity", TypeName = "numeric")]
        public decimal Capacity { get; set; } = Decimal.Zero;
    }
}
