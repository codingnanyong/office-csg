using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("box_ctrl")]
    [Index(nameof(DeviceId), Name = "box_ctrl_device_id_idx")]
    public partial class BoxCtrl
    {
        [Key]
        [Column("device_id")]
        [StringLength(20)]
        public string DeviceId { get; set; }
        [Column("t_max")]
        public float? TMax { get; set; }
        [Column("insp_r_px")]
        public int? InspRPx { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("BoxCtrl")]
        public virtual Device Device { get; set; }
    }
}
