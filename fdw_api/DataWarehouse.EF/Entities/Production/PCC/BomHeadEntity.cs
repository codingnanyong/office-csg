using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_bom_head", Schema = "pcc")]
    public class BomHeadEntity : BaseEntity
    {
        [Column("factory", TypeName = "varhcar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Column("ws_no", TypeName = "varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Column("dpa", TypeName = "varchar(20)")]
        public string Dpa { get; set; } = String.Empty;

        [Column("bom_id", TypeName = "varchar(17)")]
        public string? Id { get; set; } = String.Empty;

        [Column("bom_rev", TypeName = "varchar(10)")]
        public string? Rev { get; set; } = String.Empty;

        [Column("st_cd", TypeName = "varchar(5)")]
        public string? StCd { get; set; } = String.Empty;

        [Column("sub_st_cd", TypeName = "varchar(5)")]
        public string? SubStCd { get; set; } = String.Empty;

        [Column("season_cd", TypeName = "varchar(10)")]
        public string? Season { get; set; } = String.Empty;

        [Column("category", TypeName = "varchar(10)")]
        public string? Category { get; set; } = String.Empty;

        [Column("dev_name", TypeName = "varchar(100)")]
        public string? DevName { get; set; } = String.Empty;

        [Column("style_cd", TypeName = "varchar(15)")]
        public string? StyleCd { get; set; } = String.Empty;

        [Column("gender", TypeName = "varchar(10)")]
        public string? Gender { get; set; } = String.Empty;

        [Column("td", TypeName = "varchar(10)")]
        public string? TD { get; set; } = String.Empty;

        [Column("color_ver", TypeName = "varchar(500)")]
        public string? ColorVer { get; set; } = String.Empty;

        [Column("model_id", TypeName = "varchar(10)")]
        public string? ModelId { get; set; } = String.Empty;

        [Column("pattern_id", TypeName = "varchar(100)")]
        public string? PatternId { get; set; } = String.Empty;

        [Column("pcc_org", TypeName = "varchar(10)")]
        public string? PccOrg { get; set; } = String.Empty;

        [Column("pcc_pm", TypeName = "varchar(50)")]
        public string? PM { get; set; } = String.Empty;

        [Column("rework_yn", TypeName = "varchar(1)")]
        public string? ReworkYN { get; set; } = String.Empty;

        [Column("ws_qty", TypeName = "numeric")]
        public decimal? WsQty { get; set; } = Decimal.Zero;

        [Column("ws_status", TypeName = "varchar(1)")]
        public string? WsStatus { get; set; } = String.Empty;

        [Column("status", TypeName = "varchar(1)")]
        public string? Status { get; set; } = String.Empty;

        [Column("prod_factory", TypeName = "varchar(10)")]
        public string? ProdFactory { get; set; } = String.Empty;

        [Column("nike_send_qty", TypeName = "numeric)")]
        public decimal? NikeSendQty { get; set; } = Decimal.Zero;

        [Column("nike_dev", TypeName = "varchar(10)")]
        public string? NikeDev { get; set; } = String.Empty;

        [Column("sample_ets", TypeName = "varchar(8)")]
        public string? SampleEts { get; set; } = String.Empty;

        [Column("sample_qty", TypeName = "numeric")]
        public decimal? SampleQty { get; set; } = Decimal.Zero;

        [Column("sample_size", TypeName = "varchar(50)")]
        public string? SampleSize { get; set; } = String.Empty;

        [Column("dev_colorway_id", TypeName = "varchar(10)")]
        public string? DevColorwayId { get; set; } = String.Empty;

        [Column("dev_style_number", TypeName = "varchar(10)")]
        public string? DevStyleNumber { get; set; } = String.Empty;

    }
}
