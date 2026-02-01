using System;

namespace CSG.MI.DTO.Production
{
    public class CoordinateHistory
    {
        public string TagId { get; set; } = String.Empty;

        public string Type { get; set; } = String.Empty;

        public string Opcd { get; set; } = String.Empty;

        public string Status { get; set; } = String.Empty;

        public string Zone { get; set; } = String.Empty;

        public string Floor { get; set; } = String.Empty;

        public TimeSpan? LeadTime { get; set; } = TimeSpan.MinValue;
    }
}
