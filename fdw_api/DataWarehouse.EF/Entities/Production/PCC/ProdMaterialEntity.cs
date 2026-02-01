using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_prod_material", Schema = "pcc")]
    public class ProdMaterialEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Column("in_status", TypeName = "varchar(1)")]
        public string InStatus { get; set; } = String.Empty;

        [Column("mat_start", TypeName = "varchar(8)")]
        public string? MatStart { get; set; } = String.Empty;

        [Column("mat_date", TypeName = "varchar(8)")]
        public string? MatEnd { get; set; } = String.Empty;

        [Column("mat_status", TypeName = "varchar(1)")]
        public string? MatStatus { get; set; } = String.Empty;

        [Column("lam_date", TypeName = "varchar(8)")]
        public string? LamDate { get; set; } = String.Empty;

        [Column("lam_status", TypeName = "varchar(1)")]
        public string? LamStatus { get; set; } = String.Empty;

        [Column("swatch_date", TypeName = "varchar(8)")]
        public string? SwatchDate { get; set; } = String.Empty;

        [Column("swatch_status", TypeName = "varchar(1)")]
        public string? SwatchStatus { get; set; } = String.Empty;

        [Column("ws_error", TypeName = "varchar(500)")]
        public string? WsError { get; set; } = String.Empty;

        [Column("ws_cancel", TypeName = "varchar(10)")]
        public string? WsCancel { get; set; } = String.Empty;

        [Column("tracing_paper", TypeName = "varchar(1)")]
        public string? TracingPapper { get; set; } = String.Empty;
    }
}
