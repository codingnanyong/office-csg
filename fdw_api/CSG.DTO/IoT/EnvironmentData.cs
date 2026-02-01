using System;
namespace CSG.MI.DTO.IoT
{
    public class EnvironmentData
    {
        public DateTime MeasureTime { get; set; } = DateTime.Now;

        public float Temperature { get; set; } = 0.0f;

        public float Humidity { get; set; } = 0.0f;
    }
}
