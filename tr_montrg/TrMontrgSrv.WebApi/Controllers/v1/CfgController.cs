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
    public class CfgController : ControllerBase
    {
        private readonly ICfgRepo _repo;
        private readonly ILoggerManager _logger;

        public CfgController(ICfgRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/last/ -> cfg (the last)
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Cfg"/></returns>
        [HttpGet("{deviceId}/last")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Cfg))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetCfg(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            Cfg cfg = _repo.GetLast(deviceId);

            return Ok(cfg);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/last/[last] -> cfgs (the last n), cf. n=1: the last
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="last">Last N which is greater than zero</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.Cfg"/></returns>
        [HttpGet("{deviceId}/last/{last:int}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Cfg>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetCfgs(string deviceId, int last)
        {
            if (String.IsNullOrEmpty(deviceId) || last < 1)
                return BadRequest();

            List<Cfg> cfg = _repo.GetLast(deviceId, last);

            return Ok(cfg);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/roi/ -> rois (the last)
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns>Dictionary of ROI config</returns>
        [HttpGet("{deviceId}/roi")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Dictionary<int, int[]>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetCfgRoi(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            Cfg cfg = _repo.GetLast(deviceId);

            Dictionary<int, int[]> dic = new();

            foreach (var kvp in cfg.CfgJson.RoiCoord)
                dic.Add(Convert.ToInt32(kvp.Key), kvp.Value);

            return Ok(dic);
        }
    }
}
