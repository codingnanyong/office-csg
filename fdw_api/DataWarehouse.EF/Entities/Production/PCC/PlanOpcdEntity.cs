using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_plan_opcd", Schema = "pcc")]
    public class PlanOpcdEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Key, Column("seq", TypeName = "numeric")]
        public decimal Seq { get; set; } = Decimal.Zero;

        [Key, Column("op_cd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;

        [Column("op_qty", TypeName = "numeric")]
        public decimal Qty { get; set; } = Decimal.Zero;

        [Column("op_chk", TypeName = "varchar(1)")]
        public string? Chk { get; set; } = String.Empty;

        [Column("op_ymd", TypeName = "varchar(8)")]
        public string? Date { get; set; } = String.Empty;

        [Column("passcard", TypeName = "varchar(15)")]
        public string? Passcard { get; set; } = String.Empty;

        [Column("rework_status", TypeName = "varchar(1)")]
        public string? ReworkStat { get; set; } = String.Empty;
    }
}
