using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_plan_status", Schema = "pcc")]
    public class PlanStatusEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Key, Column("seq", TypeName = "numeric")]
        public decimal Seq { get; set; } = Decimal.Zero;

        [Column("ws_status", TypeName = "varchar(5)")]
        public string WsStatus { get; set; } = String.Empty;

        [Column("request_ymd", TypeName = "timestamp")]
        public DateTime RequestDate { get; set; } = DateTime.Now;

        [Column("plan_status", TypeName = "varchar(5)")]
        public string Status { get; set; } = String.Empty;

        [Column("plan_ymd", TypeName = "timestamp")]
        public DateTime PlnaDate { get; set; } = DateTime.Now;

        [Column("remarks", TypeName = "varchar(500)")]
        public string Remark { get; set; } = String.Empty;

        [Column("emer_yn", TypeName = "varchar(1)")]
        public string EmerYN { get; set; } = String.Empty;
    }
}
