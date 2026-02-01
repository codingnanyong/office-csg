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
    public class DeviceController : ControllerBase
    {
        private readonly IDeviceRepo _repo;
        private readonly ILoggerManager _logger;

        public DeviceController(IDeviceRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/ -> devices
        /// GET /api/v1/[controller]/?plantId=[plant_id] -> devices
        /// GET /api/v1/[controller]/?plantId=[plant_id]&locationOnly -> locations
        /// GET /api/v1/[controller]/?plantId=[plant_id]&locationId=[location_id] -> devices
        /// </summary>
        /// <param name="plantId">Plant ID</param>
        /// <param name="locationId">Location ID</param>
        /// <param name="locationOnly">Whether only location IDs returns</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.Device"/></returns>
        [HttpGet]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<Device>))]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<string>))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetDevices(string plantId, string locationId, bool locationOnly)
        {
            List<Device> devices = new();

            if (String.IsNullOrEmpty(plantId) == false && locationOnly)
            {
                var locations = _repo.FindBy(plantId)
                                     .Select(d => d.LocationId)
                                     .Distinct();
                return Ok(locations);
            }
            else if (String.IsNullOrEmpty(plantId) == false && String.IsNullOrEmpty(locationId) == false)
            {
                devices = _repo.FindBy(plantId, locationId)?.ToList();
            }
            else if (String.IsNullOrEmpty(plantId) == false)
            {
                devices = _repo.FindBy(plantId)?.ToList();
            }
            else
            {
                devices = _repo.GetAll();
            }

            return Ok(devices);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id] -> device
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Device"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetDevice(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            var itemFound = _repo.Get(deviceId);
            if (itemFound == null)
                NotFound();

            return Ok(itemFound);
        }

        /// <summary>
        /// POST /api/v1/[controller] -> device
        /// </summary>
        /// <param name="device">Device object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateDevice([FromBody] Device device)
        {
            if (device == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(device.DeviceId) ||
                String.IsNullOrWhiteSpace(device.LocationId) ||
                String.IsNullOrWhiteSpace(device.PlantId))
                ModelState.AddModelError("DeviceId", "Device ID/Location ID/Plant ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemCreated = _repo.Create(device);
            if (itemCreated == null)
                return Conflict();

            return Created("device", itemCreated);
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="device"></param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateDevice([FromBody] Device device)
        {
            if (device == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(device.DeviceId) ||
                String.IsNullOrWhiteSpace(device.LocationId) ||
                String.IsNullOrWhiteSpace(device.PlantId))
                ModelState.AddModelError("DeviceId", "Device ID/Location ID/Plant ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemUpdated = _repo.Update(device);
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
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(Device))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult DeleteDevice(string deviceId)
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
