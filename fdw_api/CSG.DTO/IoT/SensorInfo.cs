using System.Collections.Generic;
using System;

namespace CSG.MI.DTO.IoT
{
    public class SensorInfo 
    {
        public string SensorId { get; set; } = String.Empty;

        public List<EnvironmentData> Environments { get; set; } = new List<EnvironmentData>();
    }
}
