using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart.Abstract
{
    // Data Nulable Check
    public abstract class BaseWSHistoryEntity
    {
        [Column("factory", TypeName = "text")]
        public string Factory { get; set; } = String.Empty;

        [Column("wsno", TypeName = "text")]
        public string WsNo { get; set; } = String.Empty;

        [Column("opcd", TypeName = "text")]
        public string OpCd { get; set; } = String.Empty;

        [Column("opname", TypeName = "text")]
        public string? OpName { get; set; } = String.Empty;

        [Column("oplocalname", TypeName = "text")]
        public string? OpLocalName { get; set; } = String.Empty;

        [Column("opchk", TypeName = "text")]
        public string? OpChk { get; set; } = String.Empty;

        [Column("plandate", TypeName = "text")]
        public string? PlanDate { get; set; } = String.Empty;

        [Column("proddate", TypeName = "text")]
        public string? ProdDate { get; set; } = String.Empty;

        [Column("prodtime", TypeName = "text")]
        public string? ProdTime { get; set; } = String.Empty;

        [Column("status", TypeName = "text")]
        public string Status { get; set; } = String.Empty;
    }
}
