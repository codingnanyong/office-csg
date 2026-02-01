using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class EvntLog : BaseModel
    {
        public int Id { get; set; }

        public string DeviceId { get; set; }

        public string LocationId { get; set; }

        public string PlantId { get; set; }

        public string InspArea { get; set; }

        public int? AreaId { get; set; }

        public string Ymd { get; set; }

        public string Hms { get; set; }

        public DateTime EvntDt { get; set; }

        public string EvntType { get; set; }

        public string EvntLevel { get; set; }

        public string DiffLevel { get; set; }

        public float? SetValue { get; set; }

        public float? MeaValue { get; set; }

        public string Title { get; set; }

        public string Message { get; set; }

        public DateTime? EmailedDt { get; set; }

        public DateTime? UpdatedDt { get; set; }

        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }
    }
}
