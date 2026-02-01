using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.IoT
{
    [Table("sensor",Schema ="workshop")]
    public class SensorEntity : BaseEntity
    {
        [Column("sensor_id", TypeName = "varchar(20)")]
        public string SensorId { get; set; } = String.Empty;

        [Column("device_id", TypeName = "varchar(20)")]
        public string DeviceId { get; set; } = String.Empty;

        [Column("mach_id", TypeName = "varchar(30)")]
        public string MachId { get; set; } = String.Empty;

        [Column("company_cd", TypeName = "varchar(10)")]
        public string CompanyCd { get; set; } = String.Empty;

        [Column("name", TypeName = "varchar(30)")]
        public string DeviceName { get; set; } = String.Empty;

        [Column("addr", TypeName = "varchar(50)")]
        public string Address { get; set; } = String.Empty;

        [Column("topic", TypeName = "varchar(100)")]
        public string Topic { get; set; } = String.Empty;

        [Column("descn", TypeName = "varchar(200)")]
        public string Descn { get; set; } = String.Empty;
    }
}
