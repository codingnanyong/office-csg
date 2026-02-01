using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("medium")]
    [Index(nameof(DeviceId), Name = "medium_device_id_idx")]
    public partial class Medium
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
        [Column("medium_type")]
        [StringLength(10)]
        public string MediumType { get; set; }
        [Key]
        [Column("device_id")]
        [StringLength(20)]
        public string DeviceId { get; set; }
        [Column("capture_dt")]
        public DateTime CaptureDt { get; set; }
        [Required]
        [Column("file_name")]
        [StringLength(255)]
        public string FileName { get; set; }
        [Required]
        [Column("file_type")]
        [StringLength(20)]
        public string FileType { get; set; }
        [Column("file_size")]
        public long FileSize { get; set; }
        [Required]
        [Column("file_content")]
        public byte[] FileContent { get; set; }
        [Column("upd_dt")]
        public DateTime UpdDt { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("Media")]
        public virtual Device Device { get; set; }
    }
}
