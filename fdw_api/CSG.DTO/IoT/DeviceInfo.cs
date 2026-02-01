using System;
using System.Collections.Generic;

namespace CSG.MI.DTO.IoT
{
    public class DeviceInfo
    {
        public string Location { get; set; } = String.Empty;

       public List<SensorInfo> Sensors { get; set; } = new List<SensorInfo>();
    }
}