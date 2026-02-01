using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.Json;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class Cfg : BaseModel
    {
        public int Id { get; set; }

        public string DeviceId { get; set; }

        public string Ymd { get; set; }

        public string Hms { get; set; }

        public DateTime CaptureDt { get; set; }

        public ConfigJson CfgJson { get; set; }

        public DateTime? UpdatedDt { get; set; }


        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }
    }
}
