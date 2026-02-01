using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart.Abstract
{
    public abstract class BaseWSDetailEntity
    {
        [Column("factory", TypeName = "text")]
        public string Factory { get; set; } = String.Empty;

        [Column("ws_no", TypeName = "text")]
        public string WsNo { get; set; } = String.Empty;

        [Column("dpa", TypeName = "text")]
        public string? Dpa { get; set; } = String.Empty;

        [Column("bom_id", TypeName = "text")]
        public string? Id { get; set; } = String.Empty;

        [Column("bom_rev", TypeName = "text")]
        public string? Rev { get; set; } = String.Empty;

        [Column("st_cd", TypeName = "text")]
        public string? StCd { get; set; } = String.Empty;

        [Column("sub_st_cd", TypeName = "text")]
        public string? SubStCd { get; set; } = String.Empty;

        [Column("season_cd", TypeName = "text")]
        public string? Season { get; set; } = String.Empty;

        [Column("category", TypeName = "text")]
        public string? Category { get; set; } = String.Empty;

        [Column("dev_name", TypeName = "text")]
        public string? DevName { get; set; } = String.Empty;

        [Column("style_cd", TypeName = "text")]
        public string? StyleCd { get; set; } = String.Empty;

        [Column("gender", TypeName = "text")]
        public string? Gender { get; set; } = String.Empty;

        [Column("td", TypeName = "text")]
        public string? TD { get; set; } = String.Empty;

        [Column("color_ver", TypeName = "text")]
        public string? ColorVer { get; set; } = String.Empty;

        [Column("model_id", TypeName = "text")]
        public string? ModelId { get; set; } = String.Empty;

        [Column("pattern_id", TypeName = "text")]
        public string? PatternId { get; set; } = String.Empty;

        [Column("pcc_org", TypeName = "text")]
        public string? PccOrg { get; set; } = String.Empty;

        [Column("pcc_pm", TypeName = "text")]
        public string? PM { get; set; } = String.Empty;

        [Column("pm_name", TypeName = "text")]
        public string? PmName { get; set; } = String.Empty;

        [Column("rework_yn", TypeName = "text")]
        public string? ReworkYN { get; set; } = String.Empty;

        [Column("ws_qty", TypeName = "numeric")]
        public decimal? WsQty { get; set; } = Decimal.Zero;

        [Column("ws_status", TypeName = "text")]
        public string? WsStatus { get; set; } = String.Empty;

        [Column("status", TypeName = "text")]
        public string? Status { get; set; } = String.Empty;

        [Column("prod_factory", TypeName = "text")]
        public string? ProdFactory { get; set; } = String.Empty;

        [Column("nike_dev", TypeName = "text")]
        public string? NikeDev { get; set; } = String.Empty;

        [Column("nike_send_qty", TypeName = "numeric)")]
        public decimal? NikeSendQty { get; set; } = Decimal.Zero;

        [Column("sample_ets", TypeName = "text")]
        public string? SampleEts { get; set; } = String.Empty;

        [Column("sample_qty", TypeName = "numeric")]
        public decimal? SampleQty { get; set; } = Decimal.Zero;

        [Column("sample_size", TypeName = "text")]
        public string? SampleSize { get; set; } = String.Empty;

        [Column("dev_colorway_id", TypeName = "text")]
        public string? DevColorwayId { get; set; } = String.Empty;

        [Column("dev_style_number", TypeName = "text")]
        public string? DevStyleNumber { get; set; } = String.Empty;
    }
}
