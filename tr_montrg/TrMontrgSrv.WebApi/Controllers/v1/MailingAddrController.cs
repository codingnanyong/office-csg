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
    public class MailingAddrController : ControllerBase
    {
        private readonly IMailingAddrRepo _repo;
        private readonly ILoggerManager _logger;

        public MailingAddrController(IMailingAddrRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        /// <summary>
        /// GET /api/v1/[controller]/ -> active mailing_addrs
        /// </summary>
        /// <returns>List of active <see cref="CSG.MI.TrMontrgSrv.Model.MailingAddr"/></returns>
        [HttpGet()]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<MailingAddr>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetActiveMailingAddrs()
        {
            List<MailingAddr> mailingAddrs = _repo.FindAllActive();

            return Ok(mailingAddrs);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[plant_id] -> mailing_addrs
        /// </summary>
        /// <param name="plantId">Plant ID</param>
        /// <returns>List of <see cref="CSG.MI.TrMontrgSrv.Model.MailingAddr"/></returns>
        [HttpGet("{plantId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(List<MailingAddr>))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetMailingAddrs(string plantId)
        {
            if (String.IsNullOrEmpty(plantId))
                return BadRequest();

            List<MailingAddr> mailingAddrs = _repo.FindAll(plantId);

            return Ok(mailingAddrs);
        }

        /// <summary>
        /// GET /api/v1/[controller]/[plant_id]/[email] -> mailing_addr
        /// </summary>
        /// <param name="plantId">Plant ID</param>
        /// <param name="email">Email address</param>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.MailingAddr"/></returns>
        [HttpGet("{plantId}/{email}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(FrameCtrl))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetMailingAddr(string plantId, string email)
        {
            if (String.IsNullOrEmpty(plantId) || String.IsNullOrEmpty(email))
                return BadRequest();

            var mailingAddr = _repo.Get(plantId, email);

            return Ok(mailingAddr);
        }

        /// <summary>
        /// POST /api/v1/[controller] -> mailing_addr
        /// </summary>
        /// <param name="mailingAddr">MailingAddr object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.CreatedResult"/> if success</returns>
        [HttpPost]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult CreateMailingAddr([FromBody] MailingAddr mailingAddr)
        {
            if (mailingAddr == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(mailingAddr.PlantId))
                ModelState.AddModelError("PlantId", "PlantId shouldn't be empyt.");
            if (String.IsNullOrWhiteSpace(mailingAddr.Email))
                ModelState.AddModelError("Email", "Email shouldn't be empty.");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemCreated = _repo.Create(mailingAddr);
            if (itemCreated == null)
                return Conflict();

            return Created("MailingAddr", itemCreated);
        }

        /// <summary>
        /// PUT /api/v1/[controller]
        /// </summary>
        /// <param name="mailingAddr">MailingAddr object</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpPut]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(MailingAddr))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult UpdateMailingAddr([FromBody] MailingAddr mailingAddr)
        {
            if (mailingAddr == null)
                return BadRequest();

            if (String.IsNullOrWhiteSpace(mailingAddr.PlantId))
                ModelState.AddModelError("PlantId", "PlantId shouldn't be empyt.");
            if (String.IsNullOrWhiteSpace(mailingAddr.Email))
                ModelState.AddModelError("Email", "Email shouldn't be empty.");

            if (ModelState.IsValid == false)
                //return BadRequest(ModelState);
                return UnprocessableEntity(ModelState);

            var itemUpdated = _repo.Update(mailingAddr);
            if (itemUpdated == null)
                return Conflict();

            return NoContent();
        }

        /// <summary>
        /// DELETE /api/v1/[controller]/[plantId]/[email]
        /// </summary>
        /// <param name="plantId">Plant ID</param>
        /// <param name="email">Email</param>
        /// <returns><see cref="Microsoft.AspNetCore.Mvc.NoContentResult"/> if success</returns>
        [HttpDelete("{plantId}/{email}")]
        [Consumes(MediaTypeNames.Application.Json)]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(GrpKey))]
        [ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult DeleteMailingAddr(string plantId, string email)
        {
            if (String.IsNullOrEmpty(plantId) || String.IsNullOrEmpty(email))
                return BadRequest();

            var affected = _repo.Delete(plantId, email);
            if (affected == 0)
                return Conflict();

            return NoContent();
        }
    }
}
