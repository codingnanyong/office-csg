using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class Medium : BaseModel
    {
        public string Ymd { get; set; }

        public string Hms { get; set; }

        public string MediumType { get; set; }

        public string DeviceId { get; set; }

        public DateTime? CaptureDt { get; set; }

        public string FileName { get; set; }

        public string FileType { get; set; }

        public long FileSize { get; set; }

        public byte[] FileContent { get; set; }

        public DateTime? UpdatedDt { get; set; }

        // Foreign Relationship
        [JsonIgnore]
        public Device Device { get; set; }
    }
}
