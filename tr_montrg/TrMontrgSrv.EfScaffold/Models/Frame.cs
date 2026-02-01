using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("frame")]
    [Index(nameof(DeviceId), Name = "frame_device_id_idx")]
    public partial class Frame
    {
        [Key]
        [Column("ymd")]
        [StringLength(8)]
        public string Ymd { get; set; }
        [Key]
        [Column("hms")]
        [StringLength(6)]
        public string Hms { get; set; }
        [Key]
        [Column("device_id")]
        [StringLength(20)]
        public string DeviceId { get; set; }
        [Column("capture_dt")]
        public DateTime CaptureDt { get; set; }
        [Column("t_avg")]
        public float TAvg { get; set; }
        [Column("t_max")]
        public float TMax { get; set; }
        [Column("t_min")]
        public float TMin { get; set; }
        [Column("t_diff")]
        public float TDiff { get; set; }
        [Column("t_90th")]
        public float T90th { get; set; }
        [Column("t_max_x")]
        public int TMaxX { get; set; }
        [Column("t_max_y")]
        public int TMaxY { get; set; }
        [Column("upd_dt")]
        public DateTime UpdDt { get; set; }
        [Column("t_min_x")]
        public int? TMinX { get; set; }
        [Column("t_min_y")]
        public int? TMinY { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("Frames")]
        public virtual Device Device { get; set; }
    }
}
