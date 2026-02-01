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
    public class MediumController : ControllerBase
    {
        private readonly IMediumRepo _repo;
        private readonly ILoggerManager _logger;

        public MediumController(IMediumRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[medium_type]/[ymd_hms] -> medium
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="mediumType">Medium type</param>
        /// <param name="ymdhms">Date time string in yyyyMMdd_hhmmss format</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Medium"/></returns>
        [HttpGet("{deviceId}/{mediumType}/{ymdhms}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Medium))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetMedium(string deviceId, string mediumType, string ymdhms)
        {
            bool isTypeParsed = Enum.TryParse<MediumType>(mediumType, out MediumType type);
            Tuple<string, string> tp = ymdhms.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || isTypeParsed == false || tp == null)
                return BadRequest();

            var medium = _repo.Get(tp.Item1, tp.Item2, deviceId, type);

            return Ok(medium);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[medium_type]/?start=[start_ymdhms]&end=[end_ymdhms] -> media
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="mediumType">Medium type</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.Medium"/></returns>
        [HttpGet("{deviceId}/{mediumType}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Medium>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetMedia(string deviceId, string mediumType, string start, string end)
        {
            bool isTypeParsed = Enum.TryParse<MediumType>(mediumType, out MediumType type);
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || isTypeParsed == false || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<Medium> rois = _repo.FindBy(startYmdhms?.Item1,
                                             startYmdhms?.Item2,
                                             endYmdhms?.Item1,
                                             endYmdhms?.Item2,
                                             deviceId,
                                             type);
            return Ok(rois);
        }
    }
}
