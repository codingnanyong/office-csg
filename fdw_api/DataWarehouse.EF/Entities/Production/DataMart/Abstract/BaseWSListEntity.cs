using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart.Abstract
{
    public abstract class BaseWSListEntity
    {
        [Column("factory", TypeName = "text")]
        public string Factory { get; set; } = String.Empty;

        [Column("wsno", TypeName = "text")]
        public string WsNo { get; set; } = String.Empty;

        [Column("opcd", TypeName = "text")]
        public string? OpCd { get; set; } = String.Empty;

        [Column("op_name", TypeName = "text")]
        public string? OpName { get; set; } = String.Empty;

        [Column("op_local_name", TypeName = "text")]
        public string? OpLocalName { get; set; } = String.Empty;

        [Column("pm", TypeName = "text")]
        public string? Pm { get; set; } = String.Empty;

        [Column("name", TypeName = "text")]
        public string? PmName { get; set; } = String.Empty;

        [Column("model", TypeName = "text")]
        public string? Model { get; set; } = String.Empty;

        [Column("season", TypeName = "text")]
        public string? Season { get; set; } = String.Empty;

        [Column("bomid", TypeName = "text")]
        public string? BomId { get; set; } = String.Empty;

        [Column("stylecd", TypeName = "text")]
        public string? StyleCd { get; set; } = String.Empty;

        [Column("devcolorwayid", TypeName = "text")]
        public string? DevColorwayId { get; set; } = String.Empty;

        [Column("plandate", TypeName = "text")]
        public string? PlanDate { get; set; } = String.Empty;

        [Column("proddate", TypeName = "text")]
        public string? ProdDate { get; set; } = String.Empty;

        [Column("proddate_time", TypeName = "text")]
        public string? ProdTime { get; set; } = String.Empty;

        [Column("status", TypeName = "text")]
        public string Status { get; set; } = String.Empty;

        [Column("prodqty", TypeName = "numeric")]
        public decimal? ProdQty { get; set; } = Decimal.Zero;
    }
}
