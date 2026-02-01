using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_prod_scan", Schema = "pcc")]
    public class ProdScanEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Key, Column("op_cd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;

        [Key, Column("prod_seq", TypeName = "numeric")]
        public decimal? ProdSeq { get; set; } = Decimal.Zero;

        [Column("prod_qty", TypeName = "numeric")]
        public decimal? Qty { get; set; } = Decimal.Zero;

        [Column("prod_ymd", TypeName = "varchar(8)")]
        public string? Ymd { get; set; } = String.Empty;

        [Column("prod_time", TypeName = "varchar(10)")]
        public string? Time { get; set; } = String.Empty;

        [Column("prod_status", TypeName = "varchar(1)")]
        public string? Status { get; set; } = String.Empty;

        [Column("rework_status", TypeName = "varchar(1)")]
        public string? ReworkStatus { get; set; } = String.Empty;
    }
}
