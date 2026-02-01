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
    public class RoiCtrlController : ControllerBase
    {
        private readonly IRoiCtrlRepo _repo;
        private readonly IImrCtrlRepo _imrRepo;
        private readonly ILoggerManager _logger;

        public RoiCtrlController(IRoiCtrlRepo repo, IRoiImrCtrlRepo imrRepo, ILoggerManager logger)
        {
            _repo = repo;
            _imrRepo = imrRepo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id] -> roi_ctrls
        /// </summary>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.RoiCtrl"/></returns>
        [HttpGet("{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<RoiCtrl>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRoiCtrls(string deviceId)
        {
            try
            {
                if (String.IsNullOrEmpty(deviceId))
                    return BadRequest();

                List<RoiCtrl> roiCtrls = _repo.FindAll(deviceId);

                return Ok(roiCtrls);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return StatusCode(500, "Internal server error");
            }
        }

        /// <summary>
        /// GET /api/v1/[controller]/[device_id]/[roi_id] -> roi_ctrl
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="roiId">ROI ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.RoiCtrl"/></returns>
        [HttpGet("{deviceId}/{roiId:int}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(int))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRoiCtrl(string deviceId, int roiId)
        {
            try
            {
                if (String.IsNullOrEmpty(deviceId) || roiId < 0)
                    return BadRequest();

                var roiCtrl = _repo.Get(deviceId, roiId);

                return Ok(roiCtrl);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return StatusCode(500, "Internal server error");
            }
        }

        /// <summary>
        /// GET /api/v1/[controller]/imr/[device_id]/[roi_id] -> IImrCtrl
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="roiId">ROI ID</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Interface.IImrCtrl"/></returns>
        [HttpGet("imr/{deviceId}/{roiId:int}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(IImrCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(int))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetIImrCtrl(string deviceId, int roiId)
        {
            try
            {
                if (String.IsNullOrEmpty(deviceId) || roiId < 0)
                    return BadRequest();

                IImrCtrl iImrCtrl = _imrRepo.Get(deviceId, roiId);

                return Ok(iImrCtrl);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return StatusCode(500, "Internal server error");
            }
        }

        /// <summary>
        /// POST /api/v1/[controller] -> roi_ctrl
        /// </summary>
        /// <param name="roiCtrl">RoiCtrl object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateRoiCtrl([FromBody] RoiCtrl roiCtrl)
        {
            try
            {
                if (roiCtrl == null)
                    return BadRequest();

                if (String.IsNullOrWhiteSpace(roiCtrl.DeviceId))
                    ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");
                if (roiCtrl.RoiId < 0)
                    ModelState.AddModelError("RoiId", "ROI ID cann't be less than zero");

                if (ModelState.IsValid == false)
                    //return BadRequest(ModelState);
                    return UnprocessableEntity(ModelState);

                var itemCreated = _repo.Create(roiCtrl);
                if (itemCreated == null)
                    return Conflict();

                return Created("framectrl", itemCreated);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return StatusCode(500, "Internal server error");
            }
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="roiCtrl"></param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateRoiCtrl([FromBody] RoiCtrl roiCtrl)
        {
            try
            {
                if (roiCtrl == null)
                    return BadRequest();

                if (String.IsNullOrWhiteSpace(roiCtrl.DeviceId))
                    ModelState.AddModelError("DeviceId", "Device ID shouldn't be empyt");

                if (ModelState.IsValid == false)
                    //return BadRequest(ModelState);
                    return UnprocessableEntity(ModelState);

                var itemUpdated = _repo.Update(roiCtrl);
                if (itemUpdated == null)
                    return Conflict();

                return NoContent();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return StatusCode(500, "Internal server error");
            }
        }

        /// <summary>
        /// DELETE /api/v1/[controller]/[device_id]/[roi_id]
        /// </summary>
        /// <param name="deviceId">Device ID</param>
        /// <param name="roiId">ROI ID</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpDelete("{deviceId}/{roiId:int}")]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(RoiCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult DeleteRoiCtrl(string deviceId, int roiId)
        {
            try
            {
                if (String.IsNullOrEmpty(deviceId) || roiId < 0)
                    return BadRequest();

                var affected = _repo.Delete(deviceId, roiId);
                if (affected == 0)
                    return Conflict();

                return NoContent();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToFormattedString());
                return StatusCode(500, "Internal server error");
            }
        }
    }
}
