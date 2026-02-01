using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("device")]
    [Index(nameof(LocId), nameof(PlantId), Name = "device_loc_id_plant_id_idx")]
    public partial class Device
    {
        public Device()
        {
            Boxes = new HashSet<Box>();
            Cfgs = new HashSet<Cfg>();
            EvntLogs = new HashSet<EvntLog>();
            Frames = new HashSet<Frame>();
            Media = new HashSet<Medium>();
            RoiCtrls = new HashSet<RoiCtrl>();
            Rois = new HashSet<Roi>();
        }

        [Key]
        [Column("device_id")]
        [StringLength(20)]
        public string DeviceId { get; set; }
        [Required]
        [Column("loc_id")]
        [StringLength(10)]
        public string LocId { get; set; }
        [Required]
        [Column("plant_id")]
        [StringLength(10)]
        public string PlantId { get; set; }
        [Column("name")]
        [StringLength(30)]
        public string Name { get; set; }
        [Column("desn")]
        [StringLength(200)]
        public string Desn { get; set; }
        [Required]
        [Column("root_path")]
        [StringLength(255)]
        public string RootPath { get; set; }
        [Column("ord")]
        public int? Ord { get; set; }
        [Column("upd_dt")]
        public DateTime UpdDt { get; set; }
        [Column("api_port")]
        public int? ApiPort { get; set; }
        [Column("ip_addr")]
        [StringLength(45)]
        public string IpAddr { get; set; }
        [Column("ui_port")]
        public int? UiPort { get; set; }

        [InverseProperty("Device")]
        public virtual BoxCtrl BoxCtrl { get; set; }
        [InverseProperty("Device")]
        public virtual DeviceCtrl DeviceCtrl { get; set; }
        [InverseProperty("Device")]
        public virtual FrameCtrl FrameCtrl { get; set; }
        [InverseProperty(nameof(Box.Device))]
        public virtual ICollection<Box> Boxes { get; set; }
        [InverseProperty(nameof(Cfg.Device))]
        public virtual ICollection<Cfg> Cfgs { get; set; }
        [InverseProperty(nameof(EvntLog.Device))]
        public virtual ICollection<EvntLog> EvntLogs { get; set; }
        [InverseProperty(nameof(Frame.Device))]
        public virtual ICollection<Frame> Frames { get; set; }
        [InverseProperty(nameof(Medium.Device))]
        public virtual ICollection<Medium> Media { get; set; }
        [InverseProperty(nameof(RoiCtrl.Device))]
        public virtual ICollection<RoiCtrl> RoiCtrls { get; set; }
        [InverseProperty(nameof(Roi.Device))]
        public virtual ICollection<Roi> Rois { get; set; }
    }
}
