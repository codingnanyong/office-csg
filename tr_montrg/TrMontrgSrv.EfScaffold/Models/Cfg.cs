using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("cfg")]
    [Index(nameof(DeviceId), Name = "cfg_device_id_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "cfg_ymd_hms_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "ix_ymd_hms")]
    public partial class Cfg
    {
        [Key]
        [Column("id")]
        public int Id { get; set; }
        [Required]
        [Column("device_id")]
        [StringLength(20)]
        public string DeviceId { get; set; }
        [Required]
        [Column("ymd")]
        [StringLength(8)]
        public string Ymd { get; set; }
        [Required]
        [Column("hms")]
        [StringLength(6)]
        public string Hms { get; set; }
        [Column("capture_dt")]
        public DateTime CaptureDt { get; set; }
        [Required]
        [Column("cfg_file", TypeName = "jsonb")]
        public string CfgFile { get; set; }
        [Column("upd_dt")]
        public DateTime UpdDt { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("Cfgs")]
        public virtual Device Device { get; set; }
    }
}
