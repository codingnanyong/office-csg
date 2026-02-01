using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Entities
{
    [Table("frame", Schema = "public")]
    [Index(nameof(DeviceId), Name = "frame_device_id_idx")]
    public class FrameEntity : BaseEntity
    {
        [Key, Column("ymd", TypeName = "varchar(8)"), Comment("Data captured date")]
        public string Ymd { get; set; }

        [Key, Column("hms", TypeName = "varchar(6)"), Comment("Data captured time")]
        public string Hms { get; set; }

        [Key, ForeignKey(nameof(Device)), Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("capture_dt", TypeName = "timestamp"), Required, Comment("Data captured date time")]
        public DateTime? CaptureDt { get; set; }

        [Column("t_avg", TypeName = "real"), Required, Comment("Average temperature")]
        public float TAvg { get; set; }

        [Column("t_max", TypeName = "real"), Required, Comment("Max. temperature")]
        public float TMax { get; set; }

        [Column("t_min", TypeName = "real"), Required, Comment("Min. temperature")]
        public float TMin { get; set; }

        [Column("t_diff", TypeName = "real"), Required, Comment("Max. - min temperature")]
        public float TDiff { get; set; }

        [Column("t_90th", TypeName = "real"), Required, Comment("90 percentile temperature")]
        public float T90th { get; set; }

        [Column("t_max_x", TypeName = "integer"), Required, Comment("X-coordinate value of max. temperature")]
        public int TMaxX { get; set; }

        [Column("t_max_y", TypeName = "integer"), Required, Comment("Y-coordinate value of min. temperature")]
        public int TMaxY { get; set; }

        [Column("upd_dt", TypeName = "timestamp"), Required, Comment("Last updated")]
        public DateTime? UpdatedDt { get; set; }

        // v1.1
        [Column("t_min_x", TypeName = "integer"), Comment("X-coordinate value of min. temperature")]
        public int? TMinX { get; set; }

        // v1.1
        [Column("t_min_y", TypeName = "integer"), Comment("Y-coordinate value of min. temperature")]
        public int? TMinY { get; set; }

        ///////////////////////////////////////////////////////////////////////////////
        // Foreign Relationship
        [ForeignKey(nameof(DeviceId))]
        [InverseProperty(nameof(DeviceEntity.Frames))]
        public DeviceEntity Device { get; set; }

    }
}
