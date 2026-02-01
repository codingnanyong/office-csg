using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using CSG.MI.TrMontrgSrv.Model.Interface;
using CSG.MI.TrMontrgSrv.Web.Services.Interface;

namespace CSG.MI.TrMontrgSrv.Web.Montrg
{
    public class MontrgEngine
    {
        #region Fields

        private readonly IDeviceCtrlDataService _deviceCtrlDs;
        private readonly IFrameCtrlDataService _frameCtrlDs;
        private readonly IRoiCtrlDataService _roiCtrlDs;
        private IImrCtrlDataService _imrCtrlDs;

        private List<ImrData> _data;
        private Device _device;
        private InspArea _inspArea;
        private int? _areaId;

        private ImrData _lastData;
        private IImrCtrl _imrCtrl;

        #endregion

        #region Contructors

        public MontrgEngine(IDeviceCtrlDataService deviceCtrlDs,
                            IFrameCtrlDataService frameCtrlDs,
                            IRoiCtrlDataService roiCtrlDs)
        {
            _deviceCtrlDs = deviceCtrlDs;
            _frameCtrlDs = frameCtrlDs;
            _roiCtrlDs = roiCtrlDs;
        }

        #endregion

        #region Public Methods

        public async Task<List<EventData>> Validate(List<ImrData> data, Device device, InspArea inspArea, int? areaId = null)
        {
            if (data == null || data.Count == 0 || device == null || String.IsNullOrEmpty(device.DeviceId))
                return new();

            if (inspArea == InspArea.Box)
                throw new ArgumentException("InspArea.Box is not allowed.", innerException: null);

            _data = data;
            _device = device;
            _inspArea = inspArea;
            _areaId = areaId;

            _lastData = data.Last();

            if (inspArea == InspArea.Frame)
            {
                _imrCtrlDs = _frameCtrlDs;
                _imrCtrl = await _imrCtrlDs.GetImrCtrl(_device.DeviceId, null);
            }
            else if (inspArea == InspArea.ROI)
            {
                _imrCtrlDs = _roiCtrlDs;
                _imrCtrl = await _imrCtrlDs.GetImrCtrl(_device.DeviceId, _areaId);
            }

            List<EventData> eventList = new();

            // Warning temperature
            eventList.Add(CheckWarningTemp());

            // Diff. level
            eventList.Add(await CheckDiffLevel());

            // UCL/LCL of diff. and max.
            eventList.Add(CheckICtrlLimits());

            // UCL of MR of diff. and max.
            eventList.Add(CheckMrCtrlLimits());

            // Nelson rule #3
            eventList.Add(CheckNelsonRule3());

            return eventList;
        }

        #endregion

        #region Private Methods

        private EventData CheckWarningTemp()
        {
            var dt = _lastData.Dt.Value;
            var tmax = _lastData.TMax; // the last item

            var eventData = new EventData(dt, _device, _inspArea, _areaId, EventType.AboveWarning) { MeaValue = tmax };
            float? warningTemp = _imrCtrl?.TWarning;

            if (warningTemp == null)
            {
                eventData.EventLevel = EventLevel.Info;
                eventData.SetValue = null;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Warning temp. of frame is not defined" :
                                  $"Warning temp. of ROI {_areaId} is not defined";
                return eventData;
            }
            else if (tmax >= warningTemp)
            {
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = warningTemp.Value;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Temperature of frame has risen above {warningTemp.Value} ℃ (Warning temp.)" :
                                  $"Temperature of ROI {_areaId} has risen above {warningTemp.Value} ℃ (Warning temp.)";
                return eventData;
            }

            eventData.EventType = EventType.NoIssue;
            eventData.EventLevel = EventLevel.Info;
            eventData.SetValue = warningTemp.Value;
            eventData.Issue = (_inspArea == InspArea.Frame) ?
                              $"Temperature of frame is below warning temp." :
                              $"Temperature of ROI {_areaId} is below warning temp.";

            return eventData;
        }

        private async Task<EventData> CheckDiffLevel()
        {
            var ctrl = await _deviceCtrlDs.Get(_device.DeviceId);

            var levelATo = (ctrl == null) ? 10f : ctrl.LevelATo; // default 10
            var levelBTo = (ctrl == null) ? 20f : ctrl.LevelBTo; // default 20
            var levelCTo = (ctrl == null) ? 40f : ctrl.LevelCTo; // default 40

            var dt = _lastData.Dt.Value;
            var diff = _lastData.Diff;
            var diffLevel = DiffLevel.Unknown;

            if (diff < levelATo)
                diffLevel = DiffLevel.A;
            else if (diff >= levelATo && diff < levelBTo)
                diffLevel = DiffLevel.B;
            else if (diff >= levelBTo && diff < levelCTo)
                diffLevel = DiffLevel.C;
            else if (diff >= levelCTo)
                diffLevel = DiffLevel.D;

            var eventData = new EventData(dt, _device, _inspArea, _areaId, EventType.DiffLevel) { MeaValue = diff.Value };

            switch (diffLevel)
            {
                case DiffLevel.A:
                case DiffLevel.B:
                    eventData.EventLevel = EventLevel.Info;
                    eventData.SetValue = (diffLevel == DiffLevel.A) ? 0f : levelATo;
                    eventData.Issue = (diffLevel == DiffLevel.A) ?
                                      $"Diff. level is {DiffLevel.A} (PV: {diff} ℃, SV: 0 ~ {levelATo} ℃)" :
                                      $"Diff. level is {DiffLevel.B} (PV: {diff} ℃, SV: {levelATo} ~ {levelBTo} ℃)";
                    break;
                case DiffLevel.C:
                    // Check if current diff. temperature is over 60% of starting temperature of Level C
                    var warning = levelBTo + ((levelCTo - levelBTo) * 0.6);
                    eventData.EventLevel = (diff.Value <= warning) ? EventLevel.Info : EventLevel.Warning;
                    eventData.SetValue = levelBTo;
                    eventData.Issue = $"Diff. level is {DiffLevel.C} (PV: {diff} ℃, SV: {levelBTo} ~ {levelCTo} ℃)";
                    break;
                case DiffLevel.D:
                    eventData.EventLevel = EventLevel.Urgent;
                    eventData.SetValue = levelCTo;
                    eventData.Issue = $"Diff. level is {DiffLevel.D} (PV: {diff} ℃, SV: {levelCTo} ℃ ~ )";
                    break;
                default:
                    eventData.EventLevel = EventLevel.Error;
                    eventData.Issue = $"Error to define a Diff. level from {diff.Value} ℃";
                    break;
            }

            return eventData;
        }

        private EventData CheckICtrlLimits()
        {
            var dt = _lastData.Dt.Value;
            var diff = _lastData.Diff;
            var max = _lastData.TMax;
            var uclIDiff = _lastData.UclIDiff;
            var lclIDiff = _lastData.LclIDiff;
            var uclIMax = _lastData.UclIMax;
            var lclIMax = _lastData.LclIMax;

            var eventData = new EventData(dt, _device, _inspArea, _areaId, EventType.NoIssue);

            // Diff. temperature
            if (diff > uclIDiff)
            {
                eventData.EventType = EventType.AboveIUcl;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = uclIDiff;
                eventData.MeaValue = diff;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Diff temp. of frame has risen above UCL (PV: {diff} ℃, SV: {uclIDiff} ℃)" :
                                  $"Diff temp. of ROI {_areaId} has risen above UCL (PV: {diff} ℃, SV: {uclIDiff} ℃)";
                return eventData;
            }
            else if (diff < lclIDiff)
            {
                eventData.EventType = EventType.BelowILcl;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = lclIDiff;
                eventData.MeaValue = diff;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Diff temp. of frame dropped below LCL (PV: {diff} ℃, SV: {lclIDiff} ℃)" :
                                  $"Diff temp. of ROI {_areaId} dropped below LCL (PV: {diff} ℃, SV: {lclIDiff} ℃)";
                return eventData;
            }

            // Max. temperature
            if (max > uclIMax)
            {
                eventData.EventType = EventType.AboveIUcl;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = uclIMax;
                eventData.MeaValue = max;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Temperature of frame has risen above UCL (PV: {max} ℃, SV: {uclIMax} ℃)" :
                                  $"Temperature of ROI {_areaId} has risen above UCL (PV: {max} ℃, SV: {uclIMax} ℃)";
                return eventData;
            }
            else if (max < lclIMax)
            {
                eventData.EventType = EventType.BelowILcl;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = lclIMax;
                eventData.MeaValue = max;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Temperature of frame dropped below LCL (PV: {max} ℃, SV: {lclIMax} ℃)" :
                                  $"Temperature of ROI {_areaId} dropped below LCL (PV: {max} ℃, SV: {lclIMax} ℃)";
                return eventData;
            }

            eventData.EventType = EventType.NoIssue;
            eventData.EventLevel = EventLevel.Info;
            eventData.Issue = (_inspArea == InspArea.Frame) ?
                              $"Temperature of frame is normal between UCL and LCL" :
                              $"Temperature of ROI {_areaId} is normal between UCL and LCL";

            return eventData;
        }

        private EventData CheckMrCtrlLimits()
        {
            var dt = _lastData.Dt.Value;
            var mrSignDiff = _lastData.MrSignDiff;
            var mrSignMax = _lastData.MrSignMax;
            var uclMrDiff = _lastData.UclMrDiff;
            var uclMrMax = _lastData.UclMrMax;

            var eventData = new EventData(dt, _device, _inspArea, _areaId, EventType.NoIssue);

            if (mrSignDiff > uclMrDiff)
            {
                eventData.EventType = EventType.AboveMrUcl;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = uclMrDiff;
                eventData.MeaValue = mrSignDiff;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"MR of diff temp.(frame) has risen above UCL (PV: {mrSignDiff} ℃, SV: {uclMrDiff} ℃)" :
                                  $"MR of diff temp.(ROI {_areaId}) has risen above UCL (PV: {mrSignDiff} ℃, SV: {uclMrDiff} ℃)";
                return eventData;
            }

            if (mrSignMax > uclMrMax)
            {
                eventData.EventType = EventType.AboveMrUcl;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = uclMrMax;
                eventData.MeaValue = mrSignMax;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"MR of temperature(frame) has risen above UCL (PV: {mrSignMax} ℃, SV: {uclMrMax} ℃)" :
                                  $"MR of temperature(ROI {_areaId}) has risen above UCL (PV: {mrSignMax} ℃, SV: {uclMrMax} ℃)";
                return eventData;
            }

            eventData.EventType = EventType.NoIssue;
            eventData.EventLevel = EventLevel.Info;
            eventData.Issue = (_inspArea == InspArea.Frame) ?
                              $"MR of temperature of frame is normal between UCL and LCL" :
                              $"MR of temperature of ROI {_areaId} is between UCL and LCL";

            return eventData;
        }

        private EventData CheckNelsonRule3()
        {
            var dt = _lastData.Dt.Value;
            var eventData = new EventData(dt, _device, _inspArea, _areaId, EventType.NoIssue);

            if (_data.Count < 6)
            {
                eventData.EventLevel = EventLevel.Info;
                eventData.Issue = $"Lack of data points to check Nelson rule #3 (PV: {_lastData.Diff} ℃)";
                eventData.SetValue = 6;
                eventData.MeaValue = _data.Count;
                return eventData;
            }

            int lastIndex = _data.Count - 1;

            if (_data[lastIndex].MrSignDiff > _data[lastIndex - 1].MrSignDiff &&
                _data[lastIndex - 1].MrSignDiff > _data[lastIndex - 2].MrSignDiff &&
                _data[lastIndex - 2].MrSignDiff > _data[lastIndex - 3].MrSignDiff &&
                _data[lastIndex - 3].MrSignDiff > _data[lastIndex - 4].MrSignDiff &&
                _data[lastIndex - 4].MrSignDiff > _data[lastIndex - 5].MrSignDiff)
            {
                eventData.EventType = EventType.NelsonRule;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = 6;
                eventData.MeaValue = 6;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Temperature of frame is increasing continuously (PV: {_lastData.Diff} ℃)" :
                                  $"Temperature of ROI {_areaId} is increasing continuously (PV: {_lastData.Diff} ℃)";
                return eventData;
            }
            else if (_data[lastIndex].MrSignDiff < _data[lastIndex - 1].MrSignDiff &&
                _data[lastIndex - 1].MrSignDiff < _data[lastIndex - 2].MrSignDiff &&
                _data[lastIndex - 2].MrSignDiff < _data[lastIndex - 3].MrSignDiff &&
                _data[lastIndex - 3].MrSignDiff < _data[lastIndex - 4].MrSignDiff &&
                _data[lastIndex - 4].MrSignDiff < _data[lastIndex - 5].MrSignDiff)
            {
                eventData.EventType = EventType.NelsonRule;
                eventData.EventLevel = EventLevel.Warning;
                eventData.SetValue = 6;
                eventData.MeaValue = -6;
                eventData.Issue = (_inspArea == InspArea.Frame) ?
                                  $"Temperature of frame is decreasing continuously (PV: {_lastData.Diff} ℃)" :
                                  $"Temperature of ROI {_areaId} is decreasing continuously (PV: {_lastData.Diff} ℃)";
                return eventData;
            }

            eventData.EventType = EventType.NelsonRule;
            eventData.EventLevel = EventLevel.Info;
            eventData.Issue = (_inspArea == InspArea.Frame) ?
                              $"Temperature of frame is not against Nelson rule #3 (PV: {_lastData.Diff} ℃)" :
                              $"Temperature of ROI {_areaId} is not against Nelson rule #3 (PV: {_lastData.Diff} ℃)";

            return eventData;
        }

        #endregion
    }
}
