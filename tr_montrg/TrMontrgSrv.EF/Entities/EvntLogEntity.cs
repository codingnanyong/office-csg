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
    // v1.1
    [Table("evnt_log", Schema = "public")]
    [Index(nameof(DeviceId), Name = "evnt_log_device_id_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "evnt_log_ymd_hms_idx")]
    [Index(nameof(Ymd), nameof(Hms), Name = "ix_ymd_hms")]
    public class EvntLogEntity : BaseEntity
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Key, Column("id", TypeName = "integer"), Comment("Unique ID")]
        public int Id { get; set; }

        [ForeignKey(nameof(Device)), Column("device_id", TypeName = "varchar(20)"), Required, Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("loc_id", TypeName = "varchar(10)"), Required, Comment("Location ID")]
        public string LocationId { get; set; }

        [Column("plant_id", TypeName = "varchar(10)"), Required, Comment("Plant ID")]
        public string PlantId { get; set; }

        [Column("insp_area", TypeName = "varchar(10)"), Required, Comment("Inspection Area")]
        public string InspArea { get; private set; }

        [Column("area_id", TypeName = "integer"), Comment("Area ID")]
        public int? AreaId { get; private set; }

        [Column("ymd", TypeName = "varchar(8)"), Required, Comment("Event date")]
        public string Ymd { get; set; }

        [Column("hms", TypeName = "varchar(6)"), Required, Comment("Event time")]
        public string Hms { get; set; }

        [Column("evnt_dt", TypeName = "timestamp"), Required, Comment("Event date time")]
        public DateTime EvntDt { get; set; }

        [Column("evnt_type", TypeName = "varchar(15)"), Required, Comment("Type of event")]
        public string EvntType { get; set; }

        [Column("evnt_lvl", TypeName = "varchar(15)"), Required, Comment("Level of event")]
        public string EvntLevel { get; set; }

        [Column("diff_lvl", TypeName = "varchar(1)"), Required, Comment("Level of diff. temp")]
        public string DiffLevel { get; set; }

        [Column("set_value", TypeName = "real"), Comment("Setting value")]
        public float? SetValue { get; set; }

        [Column("mea_value", TypeName = "real"), Comment("Measured(actual) value")]
        public float? MeaValue { get; set; }

        [Column("title", TypeName = "varchar(100)"), Required, Comment("Title of event")]
        public string Title { get; set; }

        [Column("msg", TypeName = "varchar"), Comment("Event message")]
        public string Message { get; set; }

        [Column("emailed_dt", TypeName = "timestamp"), Comment("Emailed date time")]
        public DateTime? EmailedDt { get; set; }

        [Column("upd_dt", TypeName = "timestamp"), Required, Comment("Last updated")]
        public DateTime? UpdatedDt { get; set; }

        ///////////////////////////////////////////////////////////////////////////////
        // Foreign Relationship
        [ForeignKey(nameof(DeviceId))]
        [InverseProperty(nameof(DeviceEntity.EvntLogs))]
        public DeviceEntity Device { get; set; }
    }
}
