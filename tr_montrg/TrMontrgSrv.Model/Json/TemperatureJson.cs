using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.Json.Converters;

namespace CSG.MI.TrMontrgSrv.Model.Json
{
    public class TemperatureJson
    {
        [JsonPropertyName("plant_id")]
        public string PlantId { get; set; }

        [JsonPropertyName("location_id")]
        public string LocationId { get; set; }

        [JsonPropertyName("device_id")]
        public string DeviceId { get; set; }

        [JsonPropertyName("app_ver")]
        public string AppVer { get; set; }

        [JsonPropertyName("timestamp")]
        public string Timestamp { get; set; }

        [JsonPropertyName("frame")]
        public FrameJson Frame { get; set; }

        [JsonPropertyName("roi")]
        [JsonConverter(typeof(NonStrictArrayConverter<IReadOnlyList<RoiJson>>))]
        public IReadOnlyList<RoiJson> Roi { get; set; } = new List<RoiJson>();

        [JsonPropertyName("box")]
        [JsonConverter(typeof(NonStrictArrayConverter<IReadOnlyList<BoxJson>>))]
        public IReadOnlyList<BoxJson> Box { get; set; } = new List<BoxJson>();

        public Device ToDevice()
        {
            var device = new Device
            {
                DeviceId = DeviceId,
                LocationId = LocationId,
                PlantId = PlantId,
                Name = $"{PlantId}_{LocationId}_{DeviceId}",
                Description = null,
                RootPath = @$"D:\ftp\iotsys\{PlantId}\{LocationId}\{DeviceId}",
                Order = null,
                Frames = new List<Frame>
                {
                    new Frame
                    {
                        Ymd = Timestamp.ToDateTime().Value.ToYmdHmsTuple().Item1,
                        Hms = Timestamp.ToDateTime().Value.ToYmdHmsTuple().Item2,
                        DeviceId = DeviceId,
                        CaptureDt = Timestamp.ToDateTime(),
                        TAvg = Frame.Temp.Avg,
                        TMax = Frame.Temp.Max,
                        TMin = Frame.Temp.Min,
                        TDiff = Frame.Temp.Diff,
                        T90th = Frame.Temp.T90th,
                        TMaxX = Frame.MaxLoc.X,
                        TMaxY = Frame.MaxLoc.Y,
                    }
                }
            };

            foreach (var r in Roi)
            {
                var roi = new Roi
                {
                    Ymd = Timestamp.ToDateTime().Value.ToYmdHmsTuple().Item1,
                    Hms = Timestamp.ToDateTime().Value.ToYmdHmsTuple().Item2,
                    RoiId = r.Id,
                    DeviceId = DeviceId,
                    CaptureDt = Timestamp.ToDateTime(),
                    TAvg = r.Temp.Avg,
                    TMax = r.Temp.Max,
                    TMin = r.Temp.Min,
                    TDiff = r.Temp.Diff,
                    T90th = r.Temp.T90th,
                    TMaxX = r.MaxLoc.X,
                    TMaxY = r.MaxLoc.Y,
                    Point1X = r.Coord.X1,
                    Point1Y = r.Coord.Y1,
                    Point2X = r.Coord.X2,
                    Point2Y = r.Coord.Y2,
                };
                device.Rois.Add(roi);
            }

            foreach (var b in Box)
            {
                var box = new Box
                {
                    Ymd = Timestamp.ToDateTime().Value.ToYmdHmsTuple().Item1,
                    Hms = Timestamp.ToDateTime().Value.ToYmdHmsTuple().Item2,
                    BoxId = b.Id,
                    DeviceId = DeviceId,
                    CaptureDt = Timestamp.ToDateTime(),
                    TAvg = b.Temp.Avg,
                    TMax = b.Temp.Max,
                    TMin = b.Temp.Min,
                    TDiff = b.Temp.Diff,
                    T90th = b.Temp.T90th,
                    TMaxX = b.MaxLoc.X,
                    TMaxY = b.MaxLoc.Y,
                    Point1X = b.Coord.X1,
                    Point1Y = b.Coord.Y1,
                    Point2X = b.Coord.X2,
                    Point2Y = b.Coord.Y2,
                };
                device.Boxes.Add(box);
            }

            return device;
        }
    }

    public class FrameJson
    {
        [JsonPropertyName("temp")]
        public TempJson Temp { get; set; }

        [JsonPropertyName("max_loc")]
        public MaxLocJson MaxLoc { get; set; }
    }

    public class RoiJson
    {
        [JsonPropertyName("id")]
        public int Id { get; set; }

        [JsonPropertyName("coord")]
        public CoordJson Coord { get; set; }

        [JsonPropertyName("temp")]
        public TempJson Temp { get; set; }

        [JsonPropertyName("max_loc")]
        public MaxLocJson MaxLoc { get; set; }
    }

    public class BoxJson: RoiJson
    {

    }

    public class CoordJson
    {
        [JsonPropertyName("x1")]
        public int X1 { get; set; }

        [JsonPropertyName("y1")]
        public int Y1 { get; set; }

        [JsonPropertyName("x2")]
        public int X2 { get; set; }

        [JsonPropertyName("y2")]
        public int Y2 { get; set; }
    }

    public class TempJson
    {
        [JsonPropertyName("avg")]
        public float Avg { get; set; }

        [JsonPropertyName("max")]
        public float Max { get; set; }

        [JsonPropertyName("min")]
        public float Min { get; set; }

        [JsonPropertyName("diff")]
        public float Diff { get; set; }

        [JsonPropertyName("90th")]
        public float T90th { get; set; }
    }

    public class MaxLocJson
    {
        [JsonPropertyName("x")]
        public int X { get; set; }

        [JsonPropertyName("y")]
        public int Y { get; set; }
    }
}
