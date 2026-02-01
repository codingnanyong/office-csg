using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_plan_issue_tail", Schema = "pcc")]
    public class IssueTailEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Key, Column("op_cd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;

        [Key, Column("seq", TypeName = "numeric")]
        public decimal Seq { get; set; } = Decimal.Zero;

        [Key, Column("part_seq", TypeName = "numeric")]
        public decimal PartSeq { get; set; } = Decimal.Zero;

        [Key, Column("lam_seq", TypeName = "numeric")]
        public decimal LamSeq { get; set; } = Decimal.Zero;

        [Column("issue_lv1", TypeName = "varchar(10)")]
        public string Level1 { get; set; } = String.Empty;

        [Column("issue_lv2", TypeName = "varchar(10)")]
        public string Level2 { get; set; } = String.Empty;

        [Column("issue_lv3", TypeName = "varchar(10)")]
        public string Level3 { get; set; } = String.Empty;

        [Column("status", TypeName = "varchar(2)")]
        public string Status { get; set; } = String.Empty;

        [Column("complete_due_date", TypeName = "timestamp")]
        public DateTime CompleteDueDate { get; set; } = DateTime.Now;

        [Column("complete_date", TypeName = "timestamp")]
        public DateTime CompleteDate { get; set; } = DateTime.Now;
    }
}
