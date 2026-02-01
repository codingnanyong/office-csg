using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_plan_rework_head", Schema = "pcc")]
    public class ReworkHeadEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Key, Column("seq", TypeName = "numeric")]
        public decimal Seq { get; set; } = Decimal.Zero;    

        [Column("op_cd", TypeName = "varchar(10)")]
        public string? OpCd { get; set; } = String.Empty;

        [Column("occur_date", TypeName = "varchar(8)")]
        public string? OccurDate { get; set; } = String.Empty;

        [Column("rework_head_remarks", TypeName = "varchar(500)")]
        public string? Remark { get; set; } = String.Empty;

        [Column("complete_date", TypeName = "varchar(8)")]
        public string? CompleteDate { get; set; } = String.Empty;

        [Column("status", TypeName = "varchar(2)")]
        public string? Status { get; set; } = String.Empty;
    }
}
