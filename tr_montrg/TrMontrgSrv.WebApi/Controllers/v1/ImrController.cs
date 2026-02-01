using System;
using System.Collections.Generic;
using System.Net.Mime;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.TrMontrgSrv.WebApi.Controllers.v1
{
    [ApiController]
    [ApiVersion("1.0")]
    [Produces(MediaTypeNames.Application.Json)]
    [Route("api/v{version:apiVersion}/[controller]")]
    public class ImrController : ControllerBase
    {
        private readonly IFrameRepo _frameRepo;
        private readonly IRoiRepo _roiRepo;
        private readonly IFrameCtrlRepo _frameCtrlRepo;
        private readonly IRoiCtrlRepo _roiCtrlRepo;
        private readonly ILoggerManager _logger;

        public ImrController(IFrameRepo frameRepo, IRoiRepo roiRepo, IFrameCtrlRepo frameCtrlRepo, IRoiCtrlRepo roiCtrlRepo,
                             ILoggerManager logger)
        {
            _frameRepo = frameRepo;
            _roiRepo = roiRepo;
            _frameCtrlRepo = frameCtrlRepo;
            _roiCtrlRepo = roiCtrlRepo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/frame/?start=[start_ymdhms]&end=[end_ymdhms] -> imr_data
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.ApiData.ImrData"/></returns>
        [HttpGet("{deviceId}/frame")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<ImrData>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFrameImrChartValues(string deviceId, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<ImrData> list = _frameRepo.GetImrChartData(startYmdhms?.Item1,
                                                            startYmdhms?.Item2,
                                                            endYmdhms?.Item1,
                                                            endYmdhms?.Item2,
                                                            deviceId);
            // Overwrite UCL/LCL if custom defined
            FrameCtrl ctrl = _frameCtrlRepo.Get(deviceId);
            if (ctrl != null)
            {
                foreach (var item in list)
                {
                    item.UclIMax = ctrl.UclIMax ?? ctrl.UclIMax.Value;
                    item.LclIMax = ctrl.LclIMax ?? ctrl.LclIMax.Value;
                    item.UclMrMax = ctrl.UclMrMax ?? ctrl.UclMrMax.Value;
                    item.UclIDiff = ctrl.UclIDiff ?? ctrl.UclIDiff.Value;
                    item.LclIDiff = ctrl.LclIDiff ?? ctrl.LclIDiff.Value;
                    item.UclMrDiff = ctrl.UclMrDiff ?? ctrl.UclMrDiff.Value;
                }
            }

            return Ok(list);
        }

        /// <summary>
        ///  GET /api/v1/[controller]/[device_id]/roi/[roi_id]/?start=[start_ymdhms]&end=[end_ymdhms] -> imr_data
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="roiId">ROI ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.ApiData.ImrData"/></returns>
        [HttpGet("{deviceId}/roi/{roiId:int}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<ImrData>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRoiImrChartValues(string deviceId, int roiId, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || roiId < 0 || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<ImrData> list = _roiRepo.GetImrChartData(startYmdhms?.Item1,
                                                          startYmdhms?.Item2,
                                                          endYmdhms?.Item1,
                                                          endYmdhms?.Item2,
                                                          deviceId,
                                                          roiId);
            // Overwrite UCL/LCL if custom defined
            RoiCtrl ctrl = _roiCtrlRepo.Get(deviceId, roiId);
            if (ctrl != null)
            {
                foreach (var item in list)
                {
                    item.UclIMax = ctrl.UclIMax ?? ctrl.UclIMax.Value;
                    item.LclIMax = ctrl.LclIMax ?? ctrl.LclIMax.Value;
                    item.UclMrMax = ctrl.UclMrMax ?? ctrl.UclMrMax.Value;
                    item.UclIDiff = ctrl.UclIDiff ?? ctrl.UclIDiff.Value;
                    item.LclIDiff = ctrl.LclIDiff ?? ctrl.LclIDiff.Value;
                    item.UclMrDiff = ctrl.UclMrDiff ?? ctrl.UclMrDiff.Value;
                }
            }

            return Ok(list);
        }
    }
}
