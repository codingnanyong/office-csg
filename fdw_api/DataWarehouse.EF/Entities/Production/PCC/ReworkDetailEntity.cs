using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_plan_rework_detail", Schema = "pcc")]
    public class ReworkDetailEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Key, Column("seq", TypeName = "numeric")]
        public decimal Seq { get; set; } = Decimal.Zero;

        [Key, Column("part_seq", TypeName = "numeric")]
        public decimal PartSeq { get; set; } = Decimal.Zero;

        [Key, Column("lam_seq", TypeName = "numeric")]
        public decimal LamSeq { get; set; } = Decimal.Zero;

        [Column("rework_remarks", TypeName = "varchar(500)")]
        public string? Remark { get; set; } = String.Empty;
    }
}
