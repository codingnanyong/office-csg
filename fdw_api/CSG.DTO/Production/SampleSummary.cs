using System;
using System.Collections.Generic;

namespace CSG.MI.DTO.Production
{
    public class SampleSummary
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public string Season { get; set; } = String.Empty;

        public string Model { get; set; } = String.Empty;

        public string ColorwayId { get; set; } = String.Empty;

        public string StCd { get; set; } = String.Empty;

        public string SubStCd { get;set; } = String.Empty;

        public string Size { get; set; } = String.Empty;

        public string Gender { get; set; } = String.Empty;

        public List<SampleCoordinate> Coordinates { get; set; }
    }
}
