using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class BoxCtrl : BaseModel
    {
        [Required(ErrorMessage = "Device ID field is required.")]
        [StringLength(20, ErrorMessage = "Device ID is too long.")]
        public string DeviceId { get; set; }

        public float? TMax { get; set; }

        public float? TWarning { get; set; }

        public int? InspRadiusPixel { get; set; }

        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }
    }
}
