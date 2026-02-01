using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model.ApiData
{
    public class EventData
    {
        #region Constructors

        public EventData()
        {

        }

        public EventData(DateTime evntDt, Device device, InspArea inspArea, int? areaId, EventType eventType)
        {
            EvntDt = evntDt;
            Device = device;
            InspArea = inspArea;
            AreaId = areaId;
            EventType = eventType;
        }

        #endregion

        #region Properties

        public string EventId
        {
            get => $"{EventLevel}{EvntDt.ToYmdHms()}{Id}{EventType}";
        }

        public string Id
        {
            get => $"{Device.PlantId}.{Device.LocationId}.{Device.DeviceId}.{InspArea}{AreaId}";
        }

        public DateTime EvntDt { get; set; }

        public Device Device { get; set; }

        public InspArea InspArea { get; set; }

        public int? AreaId { get; set; }

        public EventType EventType { get; set; }

        public EventLevel EventLevel { get; set; }

        public DiffLevel DiffLevel { get; set; }

        public float? SetValue { get; set; }

        public float? MeaValue { get; set; }

        public string Issue { get; set; }

        #endregion

        #region Public Methods

        public EvntLog ToEventLog()
        {
            EvntLog evntLog = new()
            {
                DeviceId = Device.DeviceId,
                LocationId = Device.LocationId,
                PlantId = Device.PlantId,
                InspArea = InspArea.ToString(),
                AreaId = AreaId,
                Ymd = EvntDt.ToYmdHmsTuple().Item1,
                Hms = EvntDt.ToYmdHmsTuple().Item2,
                EvntDt = EvntDt,
                EvntType = EventType.ToString(),
                EvntLevel = EventLevel.ToString(),
                DiffLevel = DiffLevel.ToString(),
                SetValue = SetValue,
                MeaValue = MeaValue,
                Title = $"[{EventLevel}] {Id}.{EventType}",
                Message = Issue
            };

            return evntLog;
        }

        public override string ToString()
        {
            string txt = $"[{EventLevel}] {Id}.{EventType} @ {EvntDt.ToYmdHmsWithSeparator()} ";
            txt += $"Area:{InspArea}{AreaId}, Type:{EventType}, PV:{MeaValue}, SV:{SetValue}" + Environment.NewLine;
            txt += $"- {Issue}" + Environment.NewLine;

            return txt;
        }

        public string ToHtmlTableRow()
        {
            StringBuilder sb = new();
            sb.Append("<tr>");
            sb.Append($"<td>{Id}</td>");
            sb.Append($"<td>{EventLevel}</td>");
            sb.Append($"<td>{EventType}</td>");
            sb.Append($"<td>{MeaValue}</td>");
            sb.Append($"<td>{SetValue}</td>");
            sb.Append($"<td>{Issue}</td>");
            sb.Append("<tr/>");

            return sb.ToString();
        }

        #endregion
    }
}
