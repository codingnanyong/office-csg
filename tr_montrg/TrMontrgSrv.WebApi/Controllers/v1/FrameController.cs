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
    public class FrameController : ControllerBase
    {
        private readonly IFrameRepo _repo;
        private readonly ILoggerManager _logger;

        public FrameController(IFrameRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[ymd_hms] -> frame
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="ymdhms">Date time string in yyyyMMdd_hhmmss format</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Frame"/></returns>
        [HttpGet("{deviceId}/{ymdhms}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Frame))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFrame(string deviceId, string ymdhms)
        {
            Tuple<string, string> tp = ymdhms.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || tp == null)
                return BadRequest();

            var frame = _repo.Get(tp.Item1, tp.Item2, deviceId);

            return Ok(frame);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/?start=[start_ymdhms]&end=[end_ymdhms] -> frames
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end"></param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.Frame"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Frame>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFrames(string deviceId, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<Frame> frames = _repo.FindBy(startYmdhms?.Item1,
                                              startYmdhms?.Item2,
                                              endYmdhms?.Item1,
                                              endYmdhms?.Item2,
                                              deviceId);
            return Ok(frames);
        }
    }
}
