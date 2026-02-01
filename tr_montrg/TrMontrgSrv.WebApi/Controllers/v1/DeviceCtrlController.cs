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
    public class DeviceCtrlController : ControllerBase
    {
        private readonly IDeviceCtrlRepo _repo;
        private readonly ILoggerManager _logger;

        public DeviceCtrlController(IDeviceCtrlRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/ -> device_ctrls
        /// </summary>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.DeviceCtrl"/></returns>
        [HttpGet]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<DeviceCtrl>))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetDeviceCtrls()
        {
            List<DeviceCtrl> itemsFound = _repo.GetAll();

            return Ok(itemsFound);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id] -> device_ctrl
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.DeviceCtrl"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetDeviceCtrl(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            var itemFound = _repo.Get(deviceId);
            //if (itemFound == null)
            //    return new JsonResult(new object()); // empty json
            if (itemFound == null)
                return BadRequest();

            return Ok(itemFound);
        }

        /// <summary>
        /// POST /api/v1/[controller] -> device_ctrl
        /// </summary>
        /// <param name="deviceCtrl">DeviceCtrl object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateDeviceCtrl([FromBody] DeviceCtrl deviceCtrl)
        {
            if (deviceCtrl == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(deviceCtrl.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemCreated = _repo.Create(deviceCtrl);
            if (itemCreated == null)
                return Conflict();

            return Created("devicectrl", itemCreated);
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="deviceCtrl"></param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateDeviceCtrl([FromBody] DeviceCtrl deviceCtrl)
        {
            if (deviceCtrl == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(deviceCtrl.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemUpdated = _repo.Update(deviceCtrl);
            if (itemUpdated == null)
                return Conflict();

            return NoContent();
        }

        /// <summary>
        /// DELETE /api/v1/[controller]/[device_id]
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpDelete("{deviceId}")]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(DeviceCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult DeleteDeviceCtrl(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            var affected = _repo.Delete(deviceId);
            if (affected == 0)
                return Conflict();

            return NoContent();
        }
    }
}
