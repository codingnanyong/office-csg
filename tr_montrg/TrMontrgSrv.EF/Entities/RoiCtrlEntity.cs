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
    [Table("roi_ctrl", Schema = "public")]
    [Index(nameof(DeviceId), Name = "roi_ctrl_device_id_idx")]
    public class RoiCtrlEntity : BaseEntity
    {
        [Key, ForeignKey(nameof(Device)), Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Key, Column("roi_id", TypeName = "integer"), Comment("ROI ID")]
        public int RoiId { get; set; }

        [Column("ucl_i_max", TypeName = "real"), Comment("UCL of I-chart(max temp. base)")]
        public float? UclIMax { get; set; }

        [Column("lcl_i_max", TypeName = "real"), Comment("LCL of I-chart(max temp. base)")]
        public float? LclIMax { get; set; }

        [Column("ucl_mr_max", TypeName = "real"), Comment("UCL of MR-chart(max temp. base)")]
        public float? UclMrMax { get; set; }

        [Column("ucl_i_diff", TypeName = "real"), Comment("UCL of I-chart(diff temp. base)")]
        public float? UclIDiff { get; set; }

        [Column("lcl_i_diff", TypeName = "real"), Comment("LCL of I-chart(diff temp. base)")]
        public float? LclIDiff { get; set; }

        [Column("ucl_mr_diff", TypeName = "real"), Comment("UCL of MR-chart(diff temp. base)")]
        public float? UclMrDiff { get; set; }

        // v1.1
        [Column("t_warning", TypeName = "real"), Comment("Warning temp.")]
        public float? TWarning { get; set; }

        ///////////////////////////////////////////////////////////////////////////////
        // Foreign Relationship
        [ForeignKey(nameof(DeviceId))]
        [InverseProperty(nameof(DeviceEntity.RoiCtrls))]
        public DeviceEntity Device { get; set; }
    }
}
