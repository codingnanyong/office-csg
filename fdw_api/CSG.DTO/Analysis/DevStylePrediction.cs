using System;
using System.Collections.Generic;

namespace CSG.MI.DTO.Analysis
{
    public class DevStylePrediction
    {
        public string DevStyleNumber { get; set; } = String.Empty;

        public List<PredictionRoute> Routes { get; set; } = new List<PredictionRoute>();
    }
}
