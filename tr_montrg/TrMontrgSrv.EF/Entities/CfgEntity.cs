using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.Json;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Entities
{
    [Table("cfg", Schema = "public")]
    [Index(nameof(DeviceId), Name = "cfg_device_id_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "cfg_ymd_hms_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "ix_ymd_hms")]
    public class CfgEntity : BaseEntity
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key, Column("id",TypeName = "integer"), Comment("Unique ID")]
        public int Id { get; set; }

        [ForeignKey(nameof(Device)), Column("device_id", TypeName = "varchar(20)"), Required, Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("ymd", TypeName = "varchar(8)"), Required, Comment("Data captured date")]
        public string Ymd { get; set; }

        [Column("hms", TypeName = "varchar(6)"), Required, Comment("Data captured time")]
        public string Hms { get; set; }

        [Column("capture_dt", TypeName = "timestamp"), Required, Comment("Data captured date time")]
        public DateTime? CaptureDt { get; set; }

        [Column("cfg_file", TypeName = "jsonb"), Required, Comment("Configuration json file")]
        public ConfigJson CfgJson { get; set; }

        [Column("upd_dt", TypeName = "timestamp"), Required, Comment("Last updated")]
        public DateTime? UpdatedDt { get; set; }


        // Foreign Relationship
        [ForeignKey(nameof(DeviceId))]
        [InverseProperty(nameof(DeviceEntity.Cfgs))]
        public DeviceEntity Device { get; set; }
    }
}
