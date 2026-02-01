using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.RTLS;

[Table("tb_erp_plan_data", Schema = "rtls")]
public class TbErpPlanDataEntity : BaseEntity
{
    [Key, Column("id", TypeName = "int64")]
    public long Id { get; set; }

    [Key, Column("sys_update_dt", TypeName = "timestamp")]
    public DateTime UpdateDate { get; set; } = DateTime.Now;

    [Column("factory", TypeName = "varchar(5)")]
    public string? Factory { get; set; } = String.Empty;

    [Column("ws_no", TypeName = "varchar(20)")]
    public string? Wsno { get; set; } = String.Empty;

    [Column("pass_type", TypeName = "varchar(1)")]
    public string? PassType { get; set; } = String.Empty;

    [Column("seq", TypeName = "numeric")]
    public decimal Seq { get; set; } = Decimal.Zero;

    [Column("season", TypeName = "varchar(10)")]
    public string? Season { get; set; } = String.Empty;

    [Column("model_name", TypeName = "varchar(100)")]
    public string? Modelname { get; set; } = String.Empty;

    [Column("colorway_id", TypeName = "varchar(10)")]
    public string? ColorwayId { get; set; } = String.Empty;

    [Column("st_cd", TypeName = "varchar(50)")]
    public string? Stcd { get; set; } = String.Empty;

    [Column("sub_st_cd", TypeName = "varchar(50)")]
    public string? Substcd { get; set; } = String.Empty;

    [Column("box_no", TypeName = "varchar(15)")]
    public string? BoxNo { get; set; } = String.Empty;

    [Column("pcc_pm", TypeName = "varchar(50)")]
    public string? Pm { get; set; } = String.Empty;

    [Column("pcc_pe", TypeName = "varchar(50)")]
    public string? Pe { get; set; } = String.Empty;

    [Column("size_cd", TypeName = "varchar(50)")]
    public string? SizeCd { get; set; } = String.Empty;

    [Column("gender", TypeName = "varchar(50)")]
    public string? Gender { get; set; } = String.Empty;

    [Column("pcard_qty", TypeName = "numeric")]
    public decimal PcardQty { get; set; } = Decimal.Zero;

    [Column("sample_ets", TypeName = "varchar(8)")]
    public string SampleEts { get; set; } = String.Empty;

    [Column("sub_remark", TypeName = "varchar(400)")]
    public string? SubRemark { get; set; } = String.Empty;

    [Column("prod_factory", TypeName = "varchar(5)")]
    public string? ProdFactory { get; set; } = String.Empty;

    [Column("category", TypeName = "varchar(10)")]
    public string? Category { get; set; } = String.Empty;

    [Column("plan_date", TypeName = "varchar(8)")]
    public string? PlanDate { get; set; } = String.Empty;

    [Column("beacon_tag_id", TypeName = "varchar(50)")]
    public string? BeaconTagId { get; set; } = String.Empty;

    [Column("pcc_te", TypeName = "varchar(50)")]
    public string? Te { get; set; } = String.Empty;

    [Column("pcc_ce", TypeName = "varchar(50)")]
    public string? Ce { get; set; } = String.Empty;
}