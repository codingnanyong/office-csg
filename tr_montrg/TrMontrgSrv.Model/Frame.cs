using System;
using System.Text.Json.Serialization;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class Frame : BaseModel
    {
        public string Ymd { get; set; }

        public string Hms { get; set; }

        public string DeviceId { get; set; }

        public DateTime? CaptureDt { get; set; }

        public float TAvg { get; set; }

        public float TMax { get; set; }

        public float TMin { get; set; }

        public float TDiff { get; set; }

        public float T90th { get; set; }

        public int TMaxX { get; set; }

        public int TMaxY { get; set; }

        public DateTime? UpdatedDt { get; set; }

        public int? TMinX { get; set; }

        public int? TMinY { get; set; }


        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }

    }
}
