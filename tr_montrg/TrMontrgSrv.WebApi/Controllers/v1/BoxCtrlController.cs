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
    public class BoxCtrlController : ControllerBase
    {
        private readonly IBoxCtrlRepo _repo;
        private readonly ILoggerManager _logger;

        public BoxCtrlController(IBoxCtrlRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/ -> box_ctrls
        /// </summary>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.BoxCtrl"/></returns>
        [HttpGet]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<BoxCtrl>))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFrameCtrls()
        {
            List<BoxCtrl> boxCtrls = _repo.GetAll();

            return Ok(boxCtrls);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id] -> box_ctrl
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.BoxCtrl"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetBoxCtrl(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            var boxCtrl = _repo.Get(deviceId);

            return Ok(boxCtrl);
        }

        /// <summary>
        /// POST /api/v1/[controller] -> box_ctrl
        /// </summary>
        /// <param name="boxCtrl">BoxCtrl object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateFrameCtrl([FromBody] BoxCtrl boxCtrl)
        {
            if (boxCtrl == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(boxCtrl.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemCreated = _repo.Create(boxCtrl);
            if (itemCreated == null)
                return Conflict();

            return Created("framectrl", itemCreated);
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="boxCtrl"></param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateFrameCtrl([FromBody] BoxCtrl boxCtrl)
        {
            if (boxCtrl == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(boxCtrl.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemUpdated = _repo.Update(boxCtrl);
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
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(BoxCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult DeleteFrameCtrl(string deviceId)
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
