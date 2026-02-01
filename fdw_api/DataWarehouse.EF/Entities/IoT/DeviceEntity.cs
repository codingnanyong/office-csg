using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.IoT
{
    [Table("device",Schema ="iot")]
    public class DeviceEntity : BaseEntity
    {
        [Column("device_id",TypeName ="varchar(20)")]
        public string DeviceId { get; set; } = String.Empty;

        [Column("mach_id",TypeName ="varchar(30)")]
        public string MachId { get; set;} = String.Empty;

        [Column("company_cd",TypeName ="varchar(10)")]
        public string CompanyCd { get; set; } = String.Empty;

        [Column("name",TypeName ="varchar(30)")]
        public string DeviceName { get; set; } = String.Empty;

        [Column("descn", TypeName = "varchar(200)")]
        public string Descn { get; set; } = String.Empty;
    }
}
