using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.RTLS
{
    [Table("tb_process_loc", Schema = "rtls")]
    public class TbProcessLocEntity : BaseEntity
    {
        [Column("process_loc", TypeName = "varchar(50)")]
        public string? ProcessLoc { get; set; } = String.Empty;

        [Column("process_loc_name", TypeName = "varchar(100)")]
        public string? ProcessLocName { get; set; } = String.Empty;

        [Column("loc_name", TypeName = "varchar(100)")]
        public string? LocName { get; set; } = String.Empty;

        [Column("step_code", TypeName = "varchar(10)")]
        public string? StepCode { get; set; } = String.Empty;

        [Column("level1", TypeName = "varchar(10)")]
        public string? Level1 { get; set; } = String.Empty;

        [Column("level2", TypeName = "varchar(10)")]
        public string? Level2 { get; set; } = String.Empty;
    }
}