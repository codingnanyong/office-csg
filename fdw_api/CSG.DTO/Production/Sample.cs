using System;
using System.Collections.Generic;

namespace CSG.MI.DTO.Production
{
    public class Sample
    {
        public string Factory { get; set; } = String.Empty;

        public string WsNo { get; set; } = String.Empty;

        public SampleDetail Detail { get; set; }

        public List<SampleHist> Histories { get; set; }

        public List<SampleCoordinate> Coordinates { get; set; }
    }
}
