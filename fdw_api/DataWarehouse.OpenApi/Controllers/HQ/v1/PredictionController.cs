using CSG.MI.DTO.Analysis;
using CSG.MI.FDW.BLL.Analysis.Interface;
using CSG.MI.FDW.LoggerService;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Analysis] Prediction API.
    /// </summary>
    [ApiController]
    [Route("api/v{version:apiVersion}/[controller]")]
    [ApiVersion("1.6")]
    [ApiVersion("1.6")]
    public class PredictionController : ControllerBase
    {
        #region Constructors

        private readonly IDevStylePredictionRepo _repo;
        private readonly ILoggerManager _logger;

        /// <summary>
        /// PredictionController Constructor Definition
        /// </summary>
        /// <param name="repo">[Production] Prediction By DevStyle Repository</param>
        /// <param name="logger">Logging Manager</param>
        public PredictionController(IDevStylePredictionRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        #endregion

        #region Controllers

        /// <summary>
        /// All Prediction By DevStyle Info.
        /// </summary>
        /// <response code="200">Returns a list of development style predictions.</response>
        /// <response code="404">If no predictions are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Analysis.DevStylePrediction"/>.</returns>
        [HttpGet("devstyle")]
        [ProducesResponseType(typeof(List<DevStylePrediction>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
        {
            List<DevStylePrediction> devStylePredictions = _repo.GetDevPredictions() as List<DevStylePrediction> ?? null!;

            if (devStylePredictions == null)
            {
                return NotFound();
            }

            return Ok(devStylePredictions);
        }

        /// <summary>
        /// (async) All Prediction By DevStyle Info.
        /// </summary>
        /// <response code="200">Returns a list of development style predictions.</response>
        /// <response code="404">If no predictions are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Analysis.DevStylePrediction"/>.</returns>
        [HttpGet("devstyle/async")]
        [ProducesResponseType(typeof(List<DevStylePrediction>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
        {
            List<DevStylePrediction> devStylePredictions = await _repo.GetDevPredictionsAsync() as List<DevStylePrediction> ?? null!;

            if (devStylePredictions == null)
            {
                return NotFound();
            }

            return Ok(devStylePredictions);
        }

        /// <summary>
        /// Selected Prediction By DevStyle, Info.
        /// </summary>
        /// <response code="200">Returns a development style predictions.</response>
        /// <response code="404">If no predictions are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a active <see cref="CSG.MI.DTO.Analysis.DevStylePrediction"/>.</returns>
        [HttpGet("devstyle/{style}")]
        [ProducesResponseType(typeof(DevStylePrediction), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(string style)
        {
            DevStylePrediction devStylePrediction = _repo.GetDevPrediction(style);

            if (devStylePrediction == null)
            {
                return NotFound();
            }

            return Ok(devStylePrediction);
        }

        /// <summary>
        /// (async) Selected Prediction By DevStyle Info.
        /// </summary>
        /// <response code="200">Returns a development style predictions.</response>
        /// <response code="404">If no predictions are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a active <see cref="CSG.MI.DTO.Analysis.DevStylePrediction"/>.</returns>
        [HttpGet("devstyle/{style}/async")]
        [ProducesResponseType(typeof(DevStylePrediction), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync(string style)
        {
            DevStylePrediction devStylePrediction = await _repo.GetDevPredictionAsync(style);

            if (devStylePrediction == null)
            {
                return NotFound();
            }

            return Ok(devStylePrediction);
        }

        #endregion
    }
}
