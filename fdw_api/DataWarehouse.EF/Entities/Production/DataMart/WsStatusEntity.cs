using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("worksheets_status", Schema = "services")]
    public class WsStatusEntity : BaseEntity
    {
        [Column("opcd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;

        [Column("status", TypeName = "varchar(1)")]
        public string? Status { get; set; } = String.Empty;

        [Column("wsno", TypeName = "text")]
        public string? Ws { get; set; } = String.Empty;

        [Column("tagid", TypeName = "varchar(20)")]
        public string? TagId { get; set; } = String.Empty;

        [Column("qty", TypeName = "numeric")]
        public decimal? Qty { get; set; } = Decimal.Zero;
    }
}
