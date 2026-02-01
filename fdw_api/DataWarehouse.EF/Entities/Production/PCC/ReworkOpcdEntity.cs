using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_plan_rework_opcd", Schema = "pcc")]
    public class ReworkOpcdEntity : BaseEntity
    {

        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Key, Column("seq", TypeName = "numeric")]
        public decimal Seq { get; set; } = Decimal.Zero;

        [Key, Column("rework_op_cd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;

        [Column("rework_qty", TypeName = "numeric")]
        public decimal? Qty { get; set; } = Decimal.Zero;

        [Column("complete_date", TypeName = "varchar(8)")]
        public string CompleteDate { get; set; } = String.Empty;

        [Column("status", TypeName = "varchar(2)")]
        public string? Status { get; set; } = String.Empty;
    }
}
