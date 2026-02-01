using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.WebApi.Controllers.v1
{
    [ApiController]
    [ApiVersion("1.0")]
    [Produces(MediaTypeNames.Application.Json)]
    [Route("api/v{version:apiVersion}/[controller]")]
    public class EvntLogController : ControllerBase
    {
        private readonly IEvntLogRepo _repo;
        private readonly ILoggerManager _logger;

        public EvntLogController(IEvntLogRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/exist/[device_id]/[ymdhms] -> true/false
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="ymdhms">YMDHMS</param>
        /// <returns>True if exists</returns>
        [HttpGet("exist/{deviceId}/{ymdhms}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(bool))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Exists(string deviceId, string ymdhms)
        {
            if (String.IsNullOrEmpty(deviceId) || String.IsNullOrEmpty(ymdhms))
                return BadRequest();

            var dt = ymdhms.ToDateTime();
            if (dt == null)
                return BadRequest();

            var tp = dt.Value.ToYmdHmsTuple();

            var has = _repo.Exists(deviceId, tp.Item1, tp.Item2);

            return Ok(has);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[id] -> evnt_log
        /// </summary>
        /// <param name="Id">ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.EvntLog"/></returns>
        [HttpGet("{id:int}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(int))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLog(int id)
        {
            if (id < 0)
                return BadRequest();

            var evntLog = _repo.Get(id);

            return Ok(evntLog);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id] -> evnt_logs
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.EvntLog"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<EvntLog>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLogs(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            var eventLogs = _repo.FindBy(deviceId);

            return Ok(eventLogs);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/?start=[start_ymdhms]&end=[end_ymdhms] -> evnt_logs
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.EvntLog"/></returns>
        [HttpGet("{deviceId}/Due")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<EvntLog>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLogs(string deviceId, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<EvntLog> evntLogs = _repo.FindBy(startYmdhms?.Item1,
                                                  startYmdhms?.Item2,
                                                  endYmdhms?.Item1,
                                                  endYmdhms?.Item2,
                                                  deviceId);
            return Ok(evntLogs);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[evnt_lvl]/?start=[start_ymdhms]&end=[end_ymdhms] -> evnt_logs
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="evntLvl">Event level</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.EvntLog"/></returns>
        [HttpGet("{deviceId}/{evntLvl}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<EvntLog>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLogs(string deviceId, string evntLvl, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || String.IsNullOrEmpty(evntLvl) || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<EvntLog> evntLogs = _repo.FindBy(startYmdhms?.Item1,
                                                  startYmdhms?.Item2,
                                                  endYmdhms?.Item1,
                                                  endYmdhms?.Item2,
                                                  deviceId,
                                                  evntLvl);
            return Ok(evntLogs);
        }

        /// <summary>
        /// GET /api/v1/[controller]/?plantId=[plant_id] -> evnt_logs
        /// </summary>
        /// <param name="plantId">Plant ID</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.EvntLog"/></returns>
        [HttpGet("{plantId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Device>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLogsByPlant(string plantId)
        {
            if (String.IsNullOrEmpty(plantId))
                return BadRequest();

            var devices = _repo.FindByPlant(plantId)?.ToList();

            return Ok(devices);
        }

        /// <summary>
        /// GET /api/v1/[controller]/?plantId=[plant_id]&start=[start_ymdhms]&end=[end_ymdhms] -> evnt_logs
        /// </summary>
        /// <param name="plantId">Plant ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.EvntLog"/></returns>
        [HttpGet]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<EvntLog>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLogsByPlant(string plantId, string start, string end)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();
            Tuple<string, string> endYmdhms = end.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(plantId) || startYmdhms == null || endYmdhms == null)
                return BadRequest();

            List<EvntLog> evntLogs = _repo.FindByPlant(startYmdhms?.Item1,
                                                       startYmdhms?.Item2,
                                                       endYmdhms?.Item1,
                                                       endYmdhms?.Item2,
                                                       plantId);
            return Ok(evntLogs);
        }

        /// <summary>
        /// GET /api/v1/[controller]/latest/[plant_id]?excludingInfoLevel -> evnt_logs
        /// GET /api/v1/[controller]/latest/[plant_id]/[device_id]/?excludingInfoLevel -> evnt_logs
        /// </summary>
        /// <param name="plantId">Plant ID</param>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="end">End date time string to search in yyyyMMdd_hhmmss format</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.EvntLog"/></returns>
        [HttpGet("latest/{plantId}")]
        [HttpGet("latest/{plantId}/{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<EvntLog>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLogsLatest(string plantId, string deviceId, bool excludingInfoLevel)
        {
            if (String.IsNullOrEmpty(plantId))
                return BadRequest();

            List<EvntLog> evntLogs;

            if (String.IsNullOrEmpty(deviceId))
            {
                evntLogs = _repo.GetLastest(plantId, null, excludingInfoLevel);
                return Ok(evntLogs);
            }

            evntLogs = _repo.GetLastest(plantId, deviceId, excludingInfoLevel);

            return Ok(evntLogs);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="deviceId">Device ID</param>
        /// <returns></returns>
        [HttpGet("count/{deviceId}/{start}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(int))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetEvntLogCount(string start, string deviceId)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || startYmdhms == null )
                return BadRequest();

            var count = _repo.FindAlarmBy(deviceId, startYmdhms?.Item1, startYmdhms?.Item2);

            return Ok(count);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start">Start date time string to search in yyyyMMdd_hhmmss format</param>
        /// <param name="deviceId">Device ID</param>
        /// <returns></returns>
        [HttpGet("count/async/{deviceId}/{start}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(int))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetEvntLogCountAsync(string start, string deviceId)
        {
            Tuple<string, string> startYmdhms = start.ToYmdHmsTuple();

            if (String.IsNullOrEmpty(deviceId) || startYmdhms == null)
                return BadRequest();

            var count = await _repo.FindAlarmByAsync(deviceId, startYmdhms?.Item1, startYmdhms?.Item2);

            return Ok(count);
        }

        #region Create / Update / Delete

        /// <summary>
        /// POST /api/v1/[controller] -> evnt_log
        /// </summary>
        /// <param name="evntLog">EvntLog object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateEvntLog([FromBody] EvntLog evntLog)
        {
            if (evntLog == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(evntLog.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemCreated = _repo.Create(evntLog);
            if (itemCreated == null)
                return Conflict();

            return Created("evntlog", itemCreated);
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="evntLog"></param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(EvntLog))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateEvntLog([FromBody] EvntLog evntLog)
        {
            if (evntLog == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(evntLog.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemUpdated = _repo.Update(evntLog);
            if (itemUpdated == null)
                return Conflict();

            return NoContent();
        }

        /// <summary>
        /// DELETE /api/v1/[controller]/[id]
        /// </summary>
        /// <param name="id">Event ID</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpDelete("{id}")]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(int))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult DeleteDeviceCtrl(int id)
        {
            if (id < 0)
                return BadRequest();

            var affected = _repo.Delete(id);
            if (affected == 0)
                return Conflict();

            return NoContent();
        }

        #endregion
    }
}
