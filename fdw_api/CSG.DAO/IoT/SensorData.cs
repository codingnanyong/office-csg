using System;

namespace CSG.MI.DTO.IoT
{
    public class SensorData
    {
        public string SensorId { get; set; } = String.Empty;

        public string DeviceId { get; set; } = String.Empty;

        public float Temperature { get; set; } = 0.0f;

        public float Humidity { get; set; } = 0.0f;
    }
}
