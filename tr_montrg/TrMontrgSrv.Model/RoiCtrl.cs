using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.Interface;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class RoiCtrl : BaseModel, IImrCtrl
    {
        [Required(ErrorMessage = "Device ID field is required.")]
        [StringLength(20, ErrorMessage = "Device ID is too long.")]
        public string DeviceId { get; set; }

        [Required(ErrorMessage = "ROI ID field is required.")]
        [Range(0, 30, ErrorMessage = "ROI ID is out of range(0 ~ 30)")]
        public int RoiId { get; set; }

        public float? UclIMax { get; set; }

        public float? LclIMax { get; set; }

        public float? UclMrMax { get; set; }

        public float? UclIDiff { get; set; }

        public float? LclIDiff { get; set; }

        public float? UclMrDiff { get; set; }

        public float? TWarning { get; set; }

        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }
    }
}
