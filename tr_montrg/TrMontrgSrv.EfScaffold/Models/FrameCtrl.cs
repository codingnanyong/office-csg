using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("frame_ctrl")]
    [Index(nameof(DeviceId), Name = "frame_ctrl_device_id_idx")]
    public partial class FrameCtrl
    {
        [Key]
        [Column("device_id")]
        [StringLength(20)]
        public string DeviceId { get; set; }
        [Column("ucl_i_max")]
        public float? UclIMax { get; set; }
        [Column("lcl_i_max")]
        public float? LclIMax { get; set; }
        [Column("ucl_mr_max")]
        public float? UclMrMax { get; set; }
        [Column("ucl_i_diff")]
        public float? UclIDiff { get; set; }
        [Column("lcl_i_diff")]
        public float? LclIDiff { get; set; }
        [Column("ucl_mr_diff")]
        public float? UclMrDiff { get; set; }

        [ForeignKey(nameof(DeviceId))]
        [InverseProperty("FrameCtrl")]
        public virtual Device Device { get; set; }
    }
}
