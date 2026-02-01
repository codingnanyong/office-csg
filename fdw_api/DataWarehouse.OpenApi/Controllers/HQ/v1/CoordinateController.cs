using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
	/// [Production] Coordinate API
	/// </summary>
	[ApiController]
    [Route("api/v{version:apiVersion}/[controller]")]
    [ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class CoordinateController : ControllerBase
    {
        #region Constructors

        private readonly IWsCoordinateRepo _repo;
        private readonly ILoggerManager _logger;

        /// <summary>
        /// CoordinateController Constructor Definition
        /// </summary>
        /// <param name="repo">[Production] Coordinate Repository</param>
        /// <param name="logger">Logging Manager</param>
        public CoordinateController(IWsCoordinateRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        #endregion

        #region Controller - Coordinates

        /// <summary>
        /// All Sample Coordinate
        /// </summary>
        /// <response code="200">Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinate"/>.</response>
        /// <response code="404">If no sample coordinates are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinate"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<SampleCoordinate>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
        {
            List<SampleCoordinate> coordinates = _repo.GetCoordinates() as List<SampleCoordinate> ?? null!;

            if (coordinates == null)
            {
                return NotFound();
            }

            return Ok(coordinates);
        }

        /// <summary>
        /// (async) All Sample Coordinate
        /// </summary>
        /// <response code="200">Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinate"/>.</response>
        /// <response code="404">If no sample coordinates are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinate"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<SampleCoordinate>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
        {
            List<SampleCoordinate> coordinates = await _repo.GetCoordinatesAsync() as List<SampleCoordinate> ?? null!;

            if (coordinates == null)
            {
                return NotFound();
            }

            return Ok(coordinates);
        }

        #endregion

        #region Controller - Coordinates Histories

        /// <summary>
        /// All Sample Coordinate Histories
        /// </summary>
        /// <response code="200">Returns a list of active sample coordinate histories.</response>
        /// <response code="404">If no sample coordinate histories are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinateHistory"/>.</returns>
        [HttpGet("histories")]
        [ProducesResponseType(typeof(List<SampleCoordinateHistory>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetHistories()
        {
            List<SampleCoordinateHistory> histories = _repo.GetHistory() as List<SampleCoordinateHistory> ?? null!;

            if (histories == null)
            {
                return NotFound();
            }

            return Ok(histories);
        }

        /// <summary>
        /// (async) Sample Coordinate Histories
        /// </summary>
        /// <response code="200">Returns a list of active sample coordinate histories.</response>
        /// <response code="404">If no sample coordinate histories are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinateHistory"/>.</returns>
        [HttpGet("histories/async")]
        [ProducesResponseType(typeof(List<SampleCoordinateHistory>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetHistoriesAsync()
        {
            List<SampleCoordinateHistory> histories = await _repo.GetHistoryAsync() as List<SampleCoordinateHistory> ?? null!;

            if (histories == null)
            {
                return NotFound();
            }

            return Ok(histories);
        }

        /// <summary>
        /// All Sample By WsNo Coordinate Histories
        /// </summary>
        /// <param name="wsno">WorkSheet number.</param>
        /// <response code="200">Returns a list of active sample coordinate histories.</response>
        /// <response code="404">If no sample coordinate histories are found for the specified workstation number.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinateHistory"/>.</returns>
        [HttpGet("histories/{wsno}")]
        [ProducesResponseType(typeof(List<SampleCoordinateHistory>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetHistoriesBy(string wsno)
        {
            List<SampleCoordinateHistory> histories = _repo.GetHistoryBy(wsno) as List<SampleCoordinateHistory> ?? null!;

            if (histories == null)
            {
                return NotFound();
            }

            return Ok(histories);
        }

        /// <summary>
        /// (async) Sample By WsNo Coordinate Histories
        /// </summary>
        /// <param name="wsno">WorkSheet number.</param>
        /// <response code="200">Returns a list of active sample coordinate histories.</response>
        /// <response code="404">If no sample coordinate histories are found for the specified workstation number.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleCoordinateHistory"/>.</returns>
        [HttpGet("histories/{wsno}/async")]
        [ProducesResponseType(typeof(List<SampleCoordinateHistory>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetHistoriesByAsync(string wsno)
        {
            List<SampleCoordinateHistory> histories = await _repo.GetHistoryByAsync(wsno) as List<SampleCoordinateHistory> ?? null!;

            if (histories == null)
            {
                return NotFound();
            }

            return Ok(histories);
        }

        #endregion
    }
}
