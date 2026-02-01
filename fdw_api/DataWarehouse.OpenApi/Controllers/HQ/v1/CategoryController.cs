using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.BLL.Feedbacks.Interface;
using CSG.MI.FDW.LoggerService;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
	/// [Feedback] Category API.
	/// </summary>
	[ApiController]
    [Route("api/v{version:apiVersion}/[controller]")]
    [ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class CategoryController : ControllerBase
    {
        #region Constructors

        private readonly ICategoryRepo _repo;
        private readonly ILoggerManager _logger;

        /// <summary>
        /// CategoryController Constructor Definition
        /// </summary>
        /// <param name="repo">[Feedback] Category Repository</param>
        /// <param name="logger">Logging Manager</param>
        public CategoryController(ICategoryRepo repo, ILoggerManager logger)
        {
            _repo = repo;
            _logger = logger;
        }

        #endregion

        #region Controller

        /// <summary>
        /// All Feedback Category Info.
        /// </summary>
        /// <param name="lang">Language code (e.g., "en", "kr").</param>
        /// <response code="200">Returns a list of active <see cref="CSG.MI.DTO.Feedback.Category"/>.</response>
        /// <response code="404">If no categories are found for the specified language.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Feedback.Category"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<Category>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll([Required] string lang)
        {
            ICollection<Category> categories = _repo.Get(lang) as List<Category> ?? null!;

            if (categories == null)
            {
                return NotFound();
            }

            return Ok(categories);

        }

        /// <summary>
        /// (async) All Feedback Category Info.
        /// </summary>
        /// <param name="lang">Language code (e.g., "en", "kr").</param>
        /// <response code="200">Returns a list of active <see cref="CSG.MI.DTO.Feedback.Category"/>.</response>
        /// <response code="404">If no categories are found for the specified language.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Feedback.Category"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<Category>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync([Required] string lang)
        {
            ICollection<Category> categories = await _repo.GetAsync(lang) as List<Category> ?? null!;

            if (categories == null)
            {
                return NotFound();
            }

            return Ok(categories);
        }

        #endregion
    }
}
