using CSG.MI.DTO.IoT;
using CSG.MI.FDW.BLL.IoT.Interface;
using CSG.MI.FDW.LoggerService;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
	/// [IoT] IoT API.
	/// </summary>
    [ApiController]
    [Route("api/v{version:apiVersion}/[controller]")]
    [ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class IoTController : ControllerBase
    {
        #region Constructors

        private readonly IEnviromentRepo _repo;
        private readonly ILoggerManager _logger;

        /// <summary>
        /// IoTController Constructor Definition
        /// </summary>
        /// <param name="repo">[IoT] Enviroment Repository</param>
        /// <param name="logger">Logging Manager</param>
        public IoTController(IEnviromentRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        #endregion

        #region Controllers

        /// <summary>
        /// Current Enviroment Info by Location.
        /// </summary>
        /// <param name="loc">Location Code (1: 3D Printer Room, 2: 나염실)</param>
        /// <response code="200">Returns the current environment data.</response>
        /// <response code="404">If the current environment data is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="DeviceInfo"/>.</returns>
        [HttpGet("temperature/{loc}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetNow(int loc)
        {
            DeviceInfo environment = _repo.GetLocationEnvironment_Now(loc) ?? null!;

            if (environment == null)
            {
                return NotFound();
            }

            return Ok(environment);
        }

        /// <summary>
        /// (async) Current Enviroment Info by Location.
        /// </summary>
        /// <param name="loc">Location Code (1: 3D Printer Room, 2: 나염실)</param>
        /// <response code="200">Returns the current environment data.</response>
        /// <response code="404">If the current environment data is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="DeviceInfo"/>.</returns>
        [HttpGet("temperature/{loc}/async")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetNowAsync(int loc)
        {
            DeviceInfo environment = await _repo.GetLocationEnvironmentAsync_Now(loc) ?? null!;

            if (environment == null)
            {
                return NotFound();
            }

            return Ok(environment);
        }

        /// <summary>
        /// Today Enviroment Info by Location.
        /// </summary>
        /// <param name="loc">Location Code (1: 3D Printer Room, 2: 나염실)</param>
        /// <response code="200">Returns the current environment data.</response>
        /// <response code="404">If the current environment data is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="DeviceInfo"/>.</returns>
        [HttpGet("temperature/{loc}/today")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetToday(int loc)
        {
            DeviceInfo environment = _repo.GetLocationEnvironment_Today(loc) ?? null!;

            if (environment == null)
            {
                return NotFound();
            }

            return Ok(environment);
        }

        /// <summary>
        /// (async) Today Enviroment Info by Location.
        /// </summary>
        /// <param name="loc">Location Code (1: 3D Printer Room, 2: 나염실)</param>
        /// <response code="200">Returns the current environment data.</response>
        /// <response code="404">If the current environment data is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="DeviceInfo"/>.</returns>
        [HttpGet("temperature/{loc}/today/async")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetTodayAsync(int loc)
        {
            DeviceInfo environment = await _repo.GetLocationEnvironmentAsync_Today(loc) ?? null!;

            if (environment == null)
            {
                return NotFound();
            }

            return Ok(environment);
        }

        /// <summary>
        /// 3D Printer Room Enviroment Info Range by Date.
        /// </summary>
        /// <param name="loc">Location Code (1: 3D Printer Room, 2: 나염실)</param>
        /// <param name="start">Start date of the range (format: yyyyMMdd).</param>
        /// <param name="end">End date of the range (optional, format: yyyyMMdd, default is current date).</param>
        /// <response code="200">Returns the environment data within the specified date range.</response>
        /// <response code="400">If the start date is not provided.</response>
        /// <response code="404">If no data is found within the specified date range.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="DeviceInfo"/>.</returns>
        [HttpGet("temperature/{loc}/range")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRange(int loc,string start, string end = "")
        {
            if(String.IsNullOrEmpty(start))
            {
                return BadRequest();
            }

            DeviceInfo environments = _repo.GetLocationEnvironment_Range(loc,start,end) ?? null!;

            if (environments == null)
            {
                return NotFound();
            }

            return Ok(environments);
        }

        /// <summary>
        /// (async) 3D Printer Room Enviroment Info Range by Date.
        /// </summary>
        /// <param name="loc">Location Code (1: 3D Printer Room, 2: 나염실)</param>
        /// <param name="start">Start date of the range (format: yyyyMMdd).</param>
        /// <param name="end">End date of the range (optional, format: yyyyMMdd, default is current date).</param>
        /// <response code="200">Returns the environment data within the specified date range.</response>
        /// <response code="400">If the start date is not provided.</response>
        /// <response code="404">If no data is found within the specified date range.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="DeviceInfo"/>.</returns>
        [HttpGet("temperature/{loc}/range/async")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetRangeAsync(int loc,string start, string end = "")
        {
            if (String.IsNullOrEmpty(start))
            {
                return BadRequest();
            }

            DeviceInfo environments = await _repo.GetLocationEnvironmentAsync_Range(loc, start, end) ?? null!;

            if (environments == null)
            {
                return NotFound();
            }

            return Ok(environments);
        }

        #endregion
    }
}
