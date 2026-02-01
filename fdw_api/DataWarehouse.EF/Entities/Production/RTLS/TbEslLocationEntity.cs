using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.RTLS
{
    [Table("tb_esl_location", Schema = "rtls")]
    public class TbEslLocationEntity : BaseEntity
    {
        [Column("beacon_tag_id", TypeName = "varchar(50)")]
        public string BeaconTagId { get; set; } = String.Empty;

        [Column("process_loc", TypeName = "varchar(50)")]
        public string ProcessLoc { get; set; } = String.Empty;

        [Column("sys_update_dt", TypeName = "timestamp")]
        public DateTime UpdateDt { get; set; } = DateTime.Now;
    }
}