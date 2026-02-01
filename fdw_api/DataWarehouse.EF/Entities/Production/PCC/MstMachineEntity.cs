using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_machine_head", Schema = "pcc")]
    public class MstMachineEntity : BaseEntity
    {
        [Key, Column("seq", TypeName = "numeric")]
        public decimal Seq { get; set; } = Decimal.Zero;

        [Column("dept", TypeName = "varchar(50)")]
        public string? Dept { get; set; } = String.Empty;

        [Column("machine_place", TypeName = "varchar(100)")]
        public string? Place { get; set; } = String.Empty;

        [Column("machine_name", TypeName = "varchar(500)")]
        public string? Name { get; set; } = String.Empty;

        [Column("machine_e_name", TypeName = "varchar(500)")]
        public string? NameEng { get; set; } = String.Empty;

        [Column("op_div", TypeName = "varchar(100)")]
        public string? Division { get; set; } = String.Empty;

        [Column("op_main", TypeName = "varchar(100)")]
        public string? Main { get; set; } = String.Empty;

        [Column("op_sub", TypeName = "varchar(100)")]
        public string? Sub { get; set; } = String.Empty;

        [Column("weight", TypeName = "varchar(50)")]
        public string? Weight { get; set; } = String.Empty;

        [Column("capacitance", TypeName = "varchar(50)")]
        public string? Capacitance { get; set; } = String.Empty;

        [Column("type", TypeName = "varchar(100)")]
        public string? Type { get; set; } = String.Empty;

        [Column("dimension", TypeName = "varchar(100)")]
        public string? Dimension { get; set; } = String.Empty;

        [Column("power", TypeName = "varchar(50)")]
        public string? Power { get; set; } = String.Empty;

        [Column("asset_no", TypeName = "varchar(100)")]
        public string? AssetNo { get; set; } = String.Empty;

        [Column("machine_no", TypeName = "varchar(100)")]
        public string? MachineNo { get; set; } = String.Empty;

        [Column("grade", TypeName = "varchar(10)")]
        public string? Grade { get; set; } = String.Empty;

        [Column("use_yn", TypeName = "varchar(1)")]
        public string? UseYN { get; set; } = "Y";

        [Column("vendor", TypeName = "varchar(500)")]
        public string? Vendor { get; set; } = String.Empty;

        [Column("machine_qty", TypeName = "numeric")]
        public decimal? Qty { get; set; } = Decimal.Zero;

        [Column("machine_price", TypeName = "numeric")]
        public decimal? Price { get; set; } = Decimal.Zero;

        [Column("pur_date", TypeName = "varchar(8)")]
        public string? PurDate { get; set; } = String.Empty;

        [Column("in_date", TypeName = "varchar(8)")]
        public string? InDate { get; set; } = String.Empty;

        [Column("replace_date", TypeName = "varchar(8)")]
        public string? ReplaceDate { get; set; } = String.Empty;

        [Column("discard_date", TypeName = "varchar(8)")]
        public string? DiscardDate { get; set; } = String.Empty;

        [Column("remarks", TypeName = "varchar(500)")]
        public string? Remarks { get; set; } = String.Empty;

        [Column("status", TypeName = "varchar(1)")]
        public string? Status { get; set; } = String.Empty;
    }
}
