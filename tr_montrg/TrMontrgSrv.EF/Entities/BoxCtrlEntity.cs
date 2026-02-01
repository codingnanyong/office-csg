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
    [Table("box_ctrl", Schema = "public")]
    [Index(nameof(DeviceId), Name = "box_ctrl_device_id_idx")]
    public class BoxCtrlEntity : BaseEntity
    {
        [Key, ForeignKey(nameof(Device)), Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("t_max", TypeName = "real"), Comment("Max. temperature")]
        public float? TMax { get; set; }

        [Column("insp_r_px", TypeName = "integer"), Comment("Inspection radius in pixel")]
        public int? InspRadiusPixel { get; set; }

        // v1.1
        [Column("t_warning", TypeName = "real"), Comment("Warning temp.")]
        public float? TWarning { get; set; }

        ///////////////////////////////////////////////////////////////////////////////
        // Foreign Relationship
        [ForeignKey(nameof(DeviceId))]
        [InverseProperty(nameof(DeviceEntity.BoxCtrl))]
        public DeviceEntity Device { get; set; }
    }
}
