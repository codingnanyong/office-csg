using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("evnt_log")]
    [Index(nameof(DeviceId), Name = "evnt_log_device_id_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "evnt_log_ymd_hms_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "ix_ymd_hms1")]
    public partial class EvntLog
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
        [Column("evnt_dt")]
        public DateTime EvntDt { get; set; }
        [Required]
        [Column("evnt_type")]
        [StringLength(15)]
        public string EvntType { get; set; }
        [Required]
        [Column("evnt_lvl")]
        [StringLength(15)]
        public string EvntLvl { get; set; }
        [Column("set_value")]
        public float? SetValue { get; set; }
        [Column("mea_value")]
        public float? MeaValue { get; set; }
        [Required]
        [Column("title")]
        [StringLength(100)]
        public string Title { get; set; }
        [Column("msg", TypeName = "character varying")]
        public string Msg { get; set; }
        [Column("emailed_dt")]
        public DateTime? EmailedDt { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("EvntLogs")]
        public virtual Device Device { get; set; }
    }
}
