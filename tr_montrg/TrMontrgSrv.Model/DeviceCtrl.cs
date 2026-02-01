using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class DeviceCtrl : BaseModel
    {
        [Required(ErrorMessage = "Device ID field is required.")]
        [StringLength(20, ErrorMessage = "Device ID is too long.")]
        public string DeviceId { get; set; }

        [Required(ErrorMessage = "Level A To field is required.")]
        public float LevelATo { get; set; }

        [Required(ErrorMessage = "Level B To field is required.")]
        public float LevelBTo { get; set; }

        [Required(ErrorMessage = "Level C To field is required.")]
        public float LevelCTo { get; set; }

        [Required(ErrorMessage = "Monitoring Hour field is required.")]
        [Range(1, 24, ErrorMessage = "Monitoring Hour is out of range(1 ~ 24)")]
        public int MonitoringHour { get; set; }

        public string NelsonRule { get; set; }

        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }
    }
}
