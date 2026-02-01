using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("device_ctrl")]
    [Index(nameof(DeviceId), Name = "device_ctrl_device_id_idx")]
    public partial class DeviceCtrl
    {
        [Key]
        [Column("device_id")]
        [StringLength(20)]
        public string DeviceId { get; set; }
        [Column("lvl_a_to")]
        public float LvlATo { get; set; }
        [Column("lvl_b_to")]
        public float LvlBTo { get; set; }
        [Column("lvl_c_to")]
        public float LvlCTo { get; set; }
        [Column("montrg_hr")]
        public int MontrgHr { get; set; }
        [Column("nelson_rule")]
        [StringLength(20)]
        public string NelsonRule { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("DeviceCtrl")]
        public virtual Device Device { get; set; }
    }
}
