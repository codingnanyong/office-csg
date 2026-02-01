using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.TrMontrgSrv.WebApi.Controllers.v1
{
    [ApiController]
    [ApiVersion("1.0")]
    [Produces(MediaTypeNames.Application.Json)]
    [Route("api/v{version:apiVersion}/[controller]")]
    public class FrameCtrlController : ControllerBase
    {
        private readonly IFrameCtrlRepo _repo;
        private readonly IImrCtrlRepo _imrRepo;
        private readonly ILoggerManager _logger;

        public FrameCtrlController(IFrameCtrlRepo repo, IFrameImrCtrlRepo imrRepo, ILoggerManager logger)
        {
            _repo = repo;
            _imrRepo = imrRepo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/ -> frame_ctrls
        /// </summary>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.FrameCtrl"/></returns>
        [HttpGet]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<FrameCtrl>))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFrameCtrls()
        {
            List<FrameCtrl> frameCtrls = _repo.GetAll();

            return Ok(frameCtrls);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id] -> frame_ctrl
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.FrameCtrl"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFrameCtrl(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            var frameCtrl = _repo.Get(deviceId);

            return Ok(frameCtrl);
        }

        /// <summary>
        /// GET /api/v1/[controller]/imr/[device_id] -> IImrCtrl
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Interface.IImrCtrl"/></returns>
        [HttpGet("imr/{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(IImrCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetIImrCtrl(string deviceId)
        {
            if (String.IsNullOrEmpty(deviceId))
                return BadRequest();

            IImrCtrl imrCtrl = _imrRepo.Get(deviceId, null);

            return Ok(imrCtrl);
        }

        /// <summary>
        /// POST /api/v1/[controller] -> frame_ctrl
        /// </summary>
        /// <param name="frameCtrl">FrameCtrl object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateFrameCtrl([FromBody] FrameCtrl frameCtrl)
        {
            if (frameCtrl == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(frameCtrl.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemCreated = _repo.Create(frameCtrl);
            if (itemCreated == null)
                return Conflict();

            return Created("framectrl", itemCreated);
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="frameCtrl"></param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateFrameCtrl([FromBody] FrameCtrl frameCtrl)
        {
            if (frameCtrl == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(frameCtrl.DeviceId))
                ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemUpdated = _repo.Update(frameCtrl);
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
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FrameCtrl))]
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
