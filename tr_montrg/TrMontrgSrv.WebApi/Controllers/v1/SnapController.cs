using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
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
    public class SnapController : ControllerBase
    {
        private readonly IDeviceRepo _repo;
        private readonly ILoggerManager _logger;

        public SnapController(IDeviceRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[ymd_hms] -> device
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="ymdhms">Date time string in yyyyMMdd_hhmmss format</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Device"/></returns>
        [HttpGet("{deviceId}/{ymdhms}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSnap(string deviceId, string ymdhms)
        {
            try
            {
                Tuple<string, string> tp = ymdhms.ToYmdHmsTuple();

                if (String.IsNullOrEmpty(deviceId) || tp == null)
                    return BadRequest();

                var device = _repo.GetSnap(tp.Item1, tp.Item2, deviceId);

                // Sort by ID
                device.Rois = device.Rois.OrderBy(x => x.RoiId).ToList();
                device.Boxes = device.Boxes.OrderBy(x => x.BoxId).ToList();

                return Ok(device);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return StatusCode(500, "Internal server error");
            }
        }
    }
}
