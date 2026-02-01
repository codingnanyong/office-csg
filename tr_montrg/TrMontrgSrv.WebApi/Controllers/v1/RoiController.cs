using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.TrMontrgSrv.WebApi.Controllers.v1
{
    [ApiController]
    [ApiVersion("1.0")]
    [Produces(MediaTypeNames.Application.Json)]
    [Route("api/v{version:apiVersion}/[controller]")]
    public class RoiController : ControllerBase
    {
        private readonly IRoiRepo _repo;
        private readonly ILoggerManager _logger;

        public RoiController(IRoiRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[roi_id]/[ymd_hms] -> roi
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="roiId">ROI ID</param>
        /// <param name="ymdhms">Date time string in yyyyMMdd_hhmmss</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Roi"/></returns>
        [HttpGet("{deviceId}/{roiId:int}/{ymdhms}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Roi))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRoi(string deviceId, int roiId, string ymdhms)
        {
            Tuple<string, string> tp = ymdhms.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || roiId < 0 || tp == null)
                return BadRequest();

            var roi = _repo.Get(tp.Item1, tp.Item2, deviceId, roiId);

            return Ok(roi);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[ymd_hms] -> rois
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="ymdhms">Date time string in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.Roi"/></returns>
        [HttpGet("{deviceId}/{ymdhms}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Roi>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRois(string deviceId, string ymdhms)
        {
            Tuple<string, string> tp = ymdhms.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || tp == null)
                return BadRequest();

            var roi = _repo.FindBy(tp.Item1, tp.Item2, deviceId);

            return Ok(roi);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/?start=[start_ymdhms]&end=[end_ymdhms] -> rois
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.Roi"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Roi>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRois(string deviceId, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<Roi> rois = _repo.FindBy(startYmdhms?.Item1,
                                          startYmdhms?.Item2,
                                          endYmdhms?.Item1,
                                          endYmdhms?.Item2,
                                          deviceId);
            return Ok(rois);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[roi_id]/?start=[start_ymdhms]&end=[end_ymdhms] -> rois
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="roiId">ROI ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.Roi"/></returns>
        [HttpGet("{deviceId}/{roiId:int}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Roi>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRois(string deviceId, int roiId, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || roiId < 0 || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<Roi> rois = _repo.FindBy(startYmdhms?.Item1,
                                          startYmdhms?.Item2,
                                          endYmdhms?.Item1,
                                          endYmdhms?.Item2,
                                          deviceId,
                                          roiId);
            return Ok(rois);
        }
    }
}
