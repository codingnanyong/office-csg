using CSG.MI.TrMontrgSrv.BLL.Dashboard.Interface;
using CSG.MI.TrMontrgSrv.LoggerService;
using CSG.MI.TrMontrgSrv.Model.Dashboard;
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
    public class DashboardController : Controller
    {
        #region Fields

        private readonly IDashboardRepo _repo;
        private readonly ILoggerManager _logger;

        #endregion

        #region Constructor

        public DashboardController(IDashboardRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        #endregion

        #region Controller

        /// <summary>
        /// Current temperature information of devices.
        /// GET /api/v1/[controller] -> devices
        /// </summary>
        /// <returns>A list of <see cref="CSG.MI.TrMontrgSrv.Model.Dashboard.CurDevice"/></returns>
        [HttpGet]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
        {
            ICollection<CurDevice> devices = _repo.FindDevices();

            if (devices == null)
            {
                return NotFound();
            }

            return Ok(devices);
        }

        /// <summary>
        /// (Asynchronous) Current temperature information of devices.
        /// GET /api/v1/[controller]/async -> devices
        /// </summary>
        /// <returns><see cref="CSG.MI.TrMontrgSrv.Model.Dashboard.CurDevice"/></returns>
        [HttpGet("async")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
        {
            ICollection<CurDevice> devices = await _repo.FindDevicesAsync();

            if (devices == null)
            {
                return NotFound();
            }

            return Ok(devices);
        }

        [HttpGet("{factroy}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetByFactory(string factroy)
        {
            ICollection<CurDevice> devices = _repo.FindDeviceByFactory(factroy);

            if (devices == null)
            {
                return NotFound();
            }

            return Ok(devices);
        }

        [HttpGet("{factroy}/async")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetByFactoryAsync(string factroy)
        {
            ICollection<CurDevice> devices = await _repo.FindDeviceByFactoryAsync(factroy);

            if (devices == null)
            {
                return NotFound();
            }

            return Ok(devices);
        }


        /// <summary>
        /// Current temperature information of a specific device.
        /// GET /api/v1/[controller]/[deviceId] -> device
        /// </summary>
        /// <param name="deviceId"></param>
        /// <returns></returns>
        [HttpGet("{factory}/{deviceId}")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(string factory, string deviceId)
        {
            CurDevice device = _repo.FindDeviceById(factory, deviceId);

            if (device == null)
            {
                return NotFound();
            }

            return Ok(device);
        }

        /// <summary>
        /// (Asynchronous) Current temperature information of a specific device.
        /// GET /api/v1/[controller]/[deviceId]/async -> device
        /// </summary>
        /// <param name="deviceId"></param>
        /// <returns></returns>
        [HttpGet("{factory}/{deviceId}/async")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetlAsync(string factory, string deviceId)
        {
            CurDevice device = await _repo.FindDeviceByIdAsync(factory, deviceId);

            if (device == null)
            {
                return NotFound();
            }

            return Ok(device);
        }

        #endregion
    }
}
