using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.IoT
{
    [Table("temperature",Schema = "workshop")]
    public class TemperatureEntity : BaseEntity
    {
        [Column("ymd",TypeName ="varchar(8)")]
        public string Ymd { get; set; } = String.Empty;

        [Column("hmsf", TypeName = "varchar(12)")]
        public string Hmsf { get; set; } = String.Empty;

        [Column("sensor_id", TypeName = "varchar(20)")]
        public string SensorId { get; set; } = String.Empty;

        [Column("device_id", TypeName = "varchar(20)")]
        public string DeviceId { get; set; } = String.Empty;

        [Column("capture_dt", TypeName = "timestamptz")]
        public DateTime CaptureDate { get; set; } = DateTime.Now;

        [Column("t1",TypeName ="real")]
        public float Temperature { get; set; } = 0.0f;

        [Column("t2", TypeName = "real")]
        public float Humidity { get; set; } = 0.0f;
    }
}
