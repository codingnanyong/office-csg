using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class Device : BaseModel
    {
        [Required(ErrorMessage = "Device ID field is required.")]
        [StringLength(20, ErrorMessage = "Device ID is too long.")]
        public string DeviceId { get; set; }

        [Required(ErrorMessage = "Location ID field is required.")]
        [StringLength(10, ErrorMessage = "Location ID is too long.")]
        public string LocationId { get; set; }

        [Required(ErrorMessage = "Plant ID field is required.")]
        [StringLength(10, ErrorMessage = "Plant ID is too long.")]
        public string PlantId { get; set; }

        [StringLength(30, ErrorMessage = "Name is too long(max. 30).")]
        public string Name { get; set; }

        [StringLength(200, ErrorMessage = "Description length can't exceed 200 characters.")]
        public string Description { get; set; }

        [Required(ErrorMessage = "Root Path field is required.")]
        [StringLength(255, ErrorMessage = "Root Path length can't exceed 255 characters.")]
        public string RootPath { get; set; }

        [Range(0, 100, ErrorMessage = "Order is out of range(0 ~ 100).")]
        public int? Order { get; set; }

        public DateTime? UpdatedDt { get; set; }

        [StringLength(45, ErrorMessage = "IP Address length can't exceed 45 characters.")]
        [RegularExpression(@"^([1-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])(\.([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])){3}$", ErrorMessage = "Invalid IP address format.")]
        public string IpAddress { get; set; }

        public int? UiPort { get; set; }

        public int? ApiPort { get; set; }

        // Relationship
        public virtual ICollection<Frame> Frames { get; set; } = new HashSet<Frame>();

        public virtual ICollection<Roi> Rois { get; set; } = new HashSet<Roi>();

        public virtual ICollection<Box> Boxes { get; set; } = new HashSet<Box>();

        public virtual ICollection<Cfg> Cfgs { get; set; } = new HashSet<Cfg>();

        public virtual ICollection<Medium> Media { get; set; } = new HashSet<Medium>();

        [JsonIgnore]
        public virtual DeviceCtrl DeviceCtrl { get; set; }

        [JsonIgnore]
        public virtual FrameCtrl FrameCtrl { get; set; }

        [JsonIgnore]
        public virtual ICollection<RoiCtrl> RoiCtrls { get; set; } = new HashSet<RoiCtrl>();

        [JsonIgnore]
        public virtual BoxCtrl BoxCtrl { get; set; }

        [JsonIgnore]
        public virtual ICollection<EvntLog> EvntLogs { get; set; } = new HashSet<EvntLog>();
    }
}
