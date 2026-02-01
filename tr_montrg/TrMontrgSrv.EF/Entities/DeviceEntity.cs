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
    [Table("device", Schema = "public")]
    [Index(nameof(LocationId), nameof(PlantId), Name = "device_loc_id_plant_id_idx")]
    public class DeviceEntity : BaseEntity
    {
        [Key, Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("loc_id", TypeName = "varchar(10)"), Required, Comment("Location ID")]
        public string LocationId { get; set; }

        [Column("plant_id", TypeName = "varchar(10)"), Required, Comment("Plant ID")]
        public string PlantId { get; set; }

        [Column("name", TypeName = "varchar(30)"), Comment("Name of device")]
        public string Name { get; set; }

        [Column("desn", TypeName = "varchar(200)"), Comment("Description")]
        public string Description { get; set; }

        [Column("root_path", TypeName = "varchar(255)"), Required, Comment("Root path of data directory")]
        public string RootPath { get; set; }

        [Column("ord", TypeName = "integer"), Comment("Order by")]
        public int? Order { get; set; }

        [Column("upd_dt", TypeName = "timestamp"), Required, Comment("Last updated")]
        public DateTime? UpdatedDt { get; set; }

        // v1.1
        [Column("ip_addr", TypeName = "varchar(45)"), Comment("IP address")]
        public string IpAddress { get; set; }

        // v1.1
        [Column("ui_port", TypeName = "integer"), Comment("Port number of Web UI service")]
        public int? UiPort { get; set; }

        // v1.1
        [Column("api_port", TypeName = "integer"), Comment("Port number of RESTful API service")]
        public int? ApiPort { get; set; }

        ///////////////////////////////////////////////////////////////////////////////
        // Relationship
        [InverseProperty(nameof(FrameEntity.Device))]
        public virtual ICollection<FrameEntity> Frames { get; set; } = new HashSet<FrameEntity>();

        [InverseProperty(nameof(RoiEntity.Device))]
        public virtual ICollection<RoiEntity> Rois { get; set; } = new HashSet<RoiEntity>();

        [InverseProperty(nameof(BoxEntity.Device))]
        public virtual ICollection<BoxEntity> Boxes { get; set; } = new HashSet<BoxEntity>();

        [InverseProperty(nameof(CfgEntity.Device))]
        public virtual ICollection<CfgEntity> Cfgs { get; set; } = new HashSet<CfgEntity>();

        [InverseProperty(nameof(MediumEntity.Device))]
        public virtual ICollection<MediumEntity> Media{ get; set; } = new HashSet<MediumEntity>();

        [InverseProperty(nameof(DeviceCtrlEntity.Device))]
        public virtual DeviceCtrlEntity DeviceCtrl { get; set; }

        [InverseProperty(nameof(FrameCtrlEntity.Device))]
        public virtual FrameCtrlEntity FrameCtrl { get; set; }

        [InverseProperty(nameof(RoiCtrlEntity.Device))]
        public virtual ICollection<RoiCtrlEntity> RoiCtrls { get; set; } = new HashSet<RoiCtrlEntity>();

        [InverseProperty(nameof(BoxCtrlEntity.Device))]
        public virtual BoxCtrlEntity BoxCtrl { get; set; }

        [InverseProperty(nameof(EvntLogEntity.Device))]
        public virtual ICollection<EvntLogEntity> EvntLogs { get; set; } = new HashSet<EvntLogEntity>();
    }
}
