using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mime;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
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
    public class GrpKeyController : ControllerBase
    {
        private readonly IGrpKeyRepo _repo;
        private readonly ILoggerManager _logger;

        public GrpKeyController(IGrpKeyRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/ -> grp_keys
        /// GET /api/v1/[controller]/[group] -> grp_keys
        /// </summary>
        /// <param name="group">Group name</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.GrpKey"/></returns>
        [HttpGet]
        [HttpGet("{group}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<GrpKey>))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetGrpKeys(string group)
        {
            List<GrpKey> grpKeys = String.IsNullOrEmpty(group) ? _repo.GetAll() : _repo.FindAll(group);

            return Ok(grpKeys);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[group]/[key] -> grp_key
        /// </summary>
        /// <param name="group">Group name</param>
        /// <param name="key">Key value</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.GrpKey"/></returns>
        [HttpGet("{group}/{key}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetGrpKey(string group, string key)
        {
            if (String.IsNullOrEmpty(group) || String.IsNullOrEmpty(key))
                return BadRequest();

            var grpKey = _repo.Get(group, key);

            return Ok(grpKey);
        }

        /// <summary>
        /// POST /api/v1/[controller] -> roi_ctrl
        /// </summary>
        /// <param name="grpKey">GrpKey object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateGrpKey([FromBody] GrpKey grpKey)
        {
            if (grpKey == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(grpKey.Group))
                ModelState.AddModelError("Group", "Group shouldn't be empyt.");
            if (String.IsNullOrWhiteSpace(grpKey.Key))
                ModelState.AddModelError("Key", "Key shouldn't be empty.");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemCreated = _repo.Create(grpKey);
            if (itemCreated == null)
                return Conflict();

            return Created("GrpKey", itemCreated);
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="grpKey"></param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateGrpKey([FromBody] GrpKey grpKey)
        {
            if (grpKey == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(grpKey.Group))
                ModelState.AddModelError("Group", "Group shouldn't be empyt.");
            if (String.IsNullOrWhiteSpace(grpKey.Key))
                ModelState.AddModelError("Key", "Key shouldn't be empty.");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemUpdated = _repo.Update(grpKey);
            if (itemUpdated == null)
                return Conflict();

            return NoContent();
        }

        /// <summary>
        /// DELETE /api/v1/[controller]/[group]/[key]
        /// </summary>
        /// <param name="group">Group</param>
        /// <param name="key">Key</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpDelete("{group}/{key}")]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult DeleteGrpKey(string group, string key)
        {
            if (String.IsNullOrEmpty(group) || String.IsNullOrEmpty(key))
                return BadRequest();

            var affected = _repo.Delete(group, key);
            if (affected == 0)
                return Conflict();

            return NoContent();
        }
    }
}
