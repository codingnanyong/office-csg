using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Entities
{
    [Table("medium", Schema = "public")]
    [Index(nameof(DeviceId), Name = "medium_device_id_idx")]
    public class MediumEntity : BaseEntity
    {
        [Key, Column("ymd", TypeName = "varchar(8)"), Comment("Data captured date")]
        public string Ymd { get; set; }

        [Key, Column("hms", TypeName = "varchar(6)"), Comment("Data captured time")]
        public string Hms { get; set; }

        [Key, Column("medium_type", TypeName = "varchar(10)"), Comment("Medium type")]
        public string MediumType { get; set; }

        [Key, ForeignKey(nameof(Device)), Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("capture_dt", TypeName = "timestamp"), Required, Comment("Data captured date time")]
        public DateTime? CaptureDt { get; set; }

        [Column("file_name", TypeName = "varchar(255)"), Required, Comment("File name")]
        public string FileName { get; set; }

        [Column("file_type", TypeName = "varchar(20)"), Required, Comment("File type")]
        public string FileType { get; set; }

        [Column("file_size", TypeName = "bigint"), Required, Comment("File size in bytes")]
        public long FileSize { get; set; }

        [Column("file_content", TypeName = "bytea"), Required, Comment("File content")]
        public byte[] FileContent { get; set; }

        [Column("upd_dt", TypeName = "timestamp"), Required, Comment("Last updated")]
        public DateTime? UpdatedDt { get; set; }


        // Foreign Relationship
        [ForeignKey(nameof(DeviceId))]
        [InverseProperty(nameof(DeviceEntity.Media))]
        public DeviceEntity Device { get; set; }
    }
}
