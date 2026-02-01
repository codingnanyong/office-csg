using System;
using System.Text.Json.Serialization;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class Roi : BaseModel
    {
        public string Ymd { get; set; }

        public string Hms { get; set; }

        public int RoiId { get; set; }

        public string DeviceId { get; set; }

        public DateTime? CaptureDt { get; set; }

        public float TAvg { get; set; }

        public float TMax { get; set; }

        public float TMin { get; set; }

        public float TDiff { get; set; }

        public float T90th { get; set; }

        public int TMaxX { get; set; }

        public int TMaxY { get; set; }

        public int Point1X { get; set; }

        public int Point1Y { get; set; }

        public int Point2X { get; set; }

        public int Point2Y { get; set; }

        public DateTime? UpdatedDt { get; set; }


        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }
    }
}
