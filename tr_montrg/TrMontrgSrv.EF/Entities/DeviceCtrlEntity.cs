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
    [Table("device_ctrl", Schema = "public")]
    [Index(nameof(DeviceId), Name = "device_ctrl_device_id_idx")]
    public class DeviceCtrlEntity : BaseEntity
    {
        [Key, ForeignKey(nameof(Device)), Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("lvl_a_to", TypeName = "real"), Required, Comment("Upper limit of level A")]
        public float LevelATo { get; set; }

        [Column("lvl_b_to", TypeName = "real"), Required, Comment("Upper limit of level B")]
        public float LevelBTo { get; set; }

        [Column("lvl_c_to", TypeName = "real"), Required, Comment("Upper limit of level C")]
        public float LevelCTo { get; set; }

        [Column("montrg_hr", TypeName = "integer"), Required, Comment("Monitoring hour (up to now)")]
        public int MonitoringHour { get; set; }

        [Column("nelson_rule", TypeName = "varchar(20)"), Comment("Nelson rules to apply")]
        public string NelsonRule { get; set; }


        ///////////////////////////////////////////////////////////////////////////////
        // Foreign Relationship
        [ForeignKey(nameof(DeviceId))]
        [InverseProperty(nameof(DeviceEntity.DeviceCtrl))]
        public DeviceEntity Device { get; set; }
    }
}
