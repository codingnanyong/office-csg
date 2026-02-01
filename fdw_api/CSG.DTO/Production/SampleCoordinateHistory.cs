using System;
using System.Collections.Generic;

namespace CSG.MI.DTO.Production
{
    public class SampleCoordinateHistory
    {
        public string WsNo { get; set; } = String.Empty;

        public List<CoordinateHistory> Histories { get; set; }
    }
}
