using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("roi")]
    [Index(nameof(DeviceId), Name = "roi_device_id_idx")]
    public partial class Roi
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
        [Column("roi_id")]
        public int RoiId { get; set; }
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
        [Column("p1_x")]
        public int P1X { get; set; }
        [Column("p1_y")]
        public int P1Y { get; set; }
        [Column("p2_x")]
        public int P2X { get; set; }
        [Column("p2_y")]
        public int P2Y { get; set; }
        [Column("upd_dt")]
        public DateTime UpdDt { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("Rois")]
        public virtual Device Device { get; set; }
    }
}
