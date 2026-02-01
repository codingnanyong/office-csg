using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.IoT
{
    [Table("machine",Schema ="iot")]
    public class MachineEntity : BaseEntity
    {
        [Column("mach_id", TypeName = "varchar(30)")]
        public string MachId { get; set; } = String.Empty;

        [Column("company_cd", TypeName = "varchar(10)")]
        public string CompanyCd { get; set; } = String.Empty;

        [Column("mach_kind",TypeName ="varchar(20)")]
        public string MachKind {  get; set; } = String.Empty;

        [Column("mach_name", TypeName = "varchar(30)")]
        public string MachName {  get; set; } = String.Empty;

        [Column("descn", TypeName = "varchar(200)")]
        public string Descn { get; set; } = String.Empty;

        [Column("orgn_cd", TypeName = "varchar(10)")]
        public string OrgnCd { get; set; } = String.Empty;

        [Column("loc_cd", TypeName = "varchar(10)")]
        public string LocCd { get; set; } = String.Empty;

        [Column("line_cd", TypeName = "varchar(10)")]
        public string LineCd { get; set; } = String.Empty;

        [Column("mline_cd", TypeName = "varchar(10)")]
        public string mLineCd { get; set; } = String.Empty;

        [Column("op_cd", TypeName = "varchar(10)")]
        public string OpCd { get; set; } = String.Empty;
    }
}
