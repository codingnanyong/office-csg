using CSG.MI.TrMontrgSrv.BLL.AutoBatch.Interface;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model.Inspection;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Net.Mime;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.WebApi.Controllers.v1
{
    [ApiController]
    [ApiVersion("1.0")]
    [Produces(MediaTypeNames.Application.Json)]
    [Route("api/v{version:apiVersion}/[controller]")]
    public class InspectionController : Controller
    {
        #region Fields

        private readonly IInspectionRepo _repo;
        private readonly ILoggerManager _logger;

        #endregion

        #region Constructor

        public InspectionController(IInspectionRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        #endregion

        #region Controller

        /// <summary>
        /// This is the current device information to inspection.
        /// GET /api/v1/[controller] -> devices
        /// </summary>
        /// <returns>list of <see cref="CSG.MI.TrMontrgSrv.Model.AutoBatch.InspcDevice"/></returns>
        [HttpGet]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
        {
            ICollection<InspcDevice> devices = _repo.GetCheck();

            if (devices == null)
            {
                return NotFound();
            }

            return Ok(devices);
        }

        /// <summary>
        /// (Asynchronous) This is the current device information to inspection.
        /// GET /api/v1/[controller]/async -> devices
        /// </summary>
        /// <returns>list of <see cref="CSG.MI.TrMontrgSrv.Model.AutoBatch.InspcDevice"/></returns>
        [HttpGet("async")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
        {
            ICollection<InspcDevice> devices = await _repo.GetCheckAsync();

            if (devices == null)
            {
                return NotFound();
            }

            return Ok(devices);
        }

        /// <summary>
        /// Just Test Controller
        /// GET /api/v1/[controller]/test -> (test)devices
        /// </summary>
        /// <returns></returns>
        [HttpGet("test")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetTest()
        {
            ICollection<InspcDevice> devices = _repo.GetTest();

            if (devices == null)
            {
                return NotFound();
            }

            return Ok(devices);
        }

        #endregion
    }
}
