using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Production] Total Factory, Yearly plan-performance API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class FactoryTotalController : ControllerBase
	{
		#region Constructors

		private readonly IFactoryPlnTotalRepo _plnrepo;
		private readonly IFactoryPrfTotalRepo _prfrepo;
		private readonly ILoggerManager _logger;

        /// <summary>
        /// FactoryTotalController Constructor Definition
        /// </summary>
        /// <param name="plnrepo">[Production] Pln By Year Repository</param>
        /// <param name="prfrepo">[Production] Prf By Year Repository</param>
        /// <param name="logger">Logging Manager</param>
        public FactoryTotalController(IFactoryPlnTotalRepo plnrepo, IFactoryPrfTotalRepo prfrepo, ILoggerManager logger)
		{
			_plnrepo = plnrepo;
			_prfrepo = prfrepo;
			_logger = logger;
		}

        #endregion

        #region Pln-Total

        /// <summary>
        /// All Factory Plan to 3 years.
        /// </summary>
        /// <response code="200">Returns a list of active factory plans.</response>
        /// <response code="404">If no factory plans are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>List of active <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("plns")]
        [ProducesResponseType(typeof(List<FactoryTotal>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetPlns()
		{
			List<FactoryTotal> plns = _plnrepo.GetPlans() as List<FactoryTotal> ?? null!;

			if (plns == null)
			{
                return NotFound();
            }

			return Ok(plns);
		}

        /// <summary>
        /// (async) All Factory Plan to 3 years.
        /// </summary>
        /// <response code="200">Returns a list of active factory plans.</response>
        /// <response code="404">If no factory plans are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>List of active <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("plns/async")]
        [ProducesResponseType(typeof(List<FactoryTotal>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetPlnsAsync()
		{
			List<FactoryTotal> plns = await _plnrepo.GetPlansAsync() as List<FactoryTotal> ?? null!;

            if (plns == null)
            {
                return NotFound();
            }

            return Ok(plns);
		}

        /// <summary>
        /// Factory Plan to 3 years.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <response code="200">Returns a list of active factory plans.</response>
        /// <response code="404">If no factory plans are found for the specified factory code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>List of active <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("plns/{factory}")]
        [ProducesResponseType(typeof(List<FactoryTotal>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetPlnByFactory(string factory = "DS")
		{
			List<FactoryTotal> plns = _plnrepo.GetByFactory(factory) as List<FactoryTotal> ?? null!;

			if (plns == null)
            {
                return NotFound();
            }

            return Ok(plns);
		}

        /// <summary>
        /// (async) Factory Plan to 3 years.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <response code="200">Returns a list of active factory plans.</response>
        /// <response code="404">If no factory plans are found for the specified factory code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>List of active <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("plns/{factory}/async")]
        [ProducesResponseType(typeof(List<FactoryTotal>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetPlnByFactoryAsync(string factory = "DS")
		{
			List<FactoryTotal> plns = await _plnrepo.GetByFactoryAsync(factory) as List<FactoryTotal> ?? null!;

			if (plns == null)
            {
                return NotFound();
            }

            return Ok(plns);
		}

        /// <summary>
        /// Factory Plan to target year.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="year">Target year.</param>
        /// <response code="200">Returns the factory plan for the specified year.</response>
        /// <response code="404">If no factory plan is found for the specified factory code and year.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Factory plan for the specified year <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("plns/{factory}/{year}")]
        [ProducesResponseType(typeof(FactoryTotal),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetPlnByFactoryToYear(string year, string factory = "DS")
		{
			FactoryTotal pln = _plnrepo.GetByYearFactory(year, factory);

			if (pln == null)
            {
                return NotFound();
            }

            return Ok(pln);
		}

        /// <summary>
        /// (async) Factory Plan to target year.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="year">Target year.</param>
        /// <response code="200">Returns the factory plan for the specified year.</response>
        /// <response code="404">If no factory plan is found for the specified factory code and year.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Factory plan for the specified year <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("plns/{factory}/{year}/async")]
        [ProducesResponseType(typeof(FactoryTotal), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetPlnByFactoryToYearAsync(string year, string factory = "DS")
		{
			FactoryTotal pln = await _plnrepo.GetByYearFactoryAsync(year, factory);

            if (pln == null)
            {
                return NotFound();
            }

            return Ok(pln);
		}

        #endregion

        #region Prf-Total

        /// <summary>
        /// All Factory Performace to 3 years.
        /// </summary>
        /// <response code="200">Returns a list of active factory performance data.</response>
        /// <response code="404">If no factory performance data is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active<see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("prfs")]
        [ProducesResponseType(typeof(List<FactoryTotal>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetPrfs()
		{
			List<FactoryTotal> prfs = _prfrepo.GetPerformaces() as List<FactoryTotal> ?? null!;

			if (prfs == null)
            {
                return NotFound();
            }

            return Ok(prfs);
		}

        /// <summary>
        /// (async) All Factory Performace to 3 years.
        /// </summary>
        /// <response code="200">Returns a list of active factory performance data.</response>
        /// <response code="404">If no factory performance data is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("prfs/async")]
        [ProducesResponseType(typeof(List<FactoryTotal>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetPrfsAsync()
		{
			List<FactoryTotal> prfs = await _prfrepo.GetPerformacesAsync() as List<FactoryTotal> ?? null!;

			if (prfs == null)
            {
                return NotFound();
            }

            return Ok(prfs);
		}

        /// <summary>
        /// Factory Performace to 3 years.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <response code="200">Returns a list of active factory performance data.</response>
        /// <response code="404">If no factory performance data is found for the specified factory.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("prfs/{factory}")]
        [ProducesResponseType(typeof(List<FactoryTotal>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetPrfByFactory(string factory = "DS")
		{
			List<FactoryTotal> prfs = _prfrepo.GetByFactory(factory) as List<FactoryTotal> ?? null!;

			if (prfs == null)
            {
                return NotFound();
            }

            return Ok(prfs);
		}

        /// <summary>
        /// (async) Factory Performace to 3 years.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <response code="200">Returns a list of active factory performance data.</response>
        /// <response code="404">If no factory performance data is found for the specified factory.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("prfs/{factory}/async")]
        [ProducesResponseType(typeof(List<FactoryTotal>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetPrfByFactoryAsync(string factory = "DS")
		{
			List<FactoryTotal> prfs = await _prfrepo.GetByFactoryAsync(factory) as List<FactoryTotal> ?? null!;

			if (prfs == null)
            {
                return NotFound();
            }

            return Ok(prfs);
		}

        /// <summary>
        /// Factory Performace to target year.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="year">Target year.</param>
        /// <response code="200">Returns active factory performance data for the specified year.</response>
        /// <response code="404">If no factory performance data is found for the specified factory and year.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("prfs/{factory}/{year}")]
        [ProducesResponseType(typeof(FactoryTotal),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetPrfByFactoryToYear(string year, string factory = "DS")
		{
			FactoryTotal prf = _prfrepo.GetByYearFactory(year, factory);

			if (prf == null)
            {
                return NotFound();
            }

            return Ok(prf);
		}

        /// <summary>
        /// (async) Factory Performace to target year.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="year">Target year.</param>
        /// <response code="200">Returns active factory performance data for the specified year.</response>
        /// <response code="404">If no factory performance data is found for the specified factory and year.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.FactoryTotal"/>.</returns>
        [HttpGet("prfs/{factory}/{year}/async")]
        [ProducesResponseType(typeof(FactoryTotal), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetPrfByFactoryToYearAsync(string year, string factory = "DS")
		{
			FactoryTotal prf = await _prfrepo.GetByYearFactoryAsync(year, factory);

			if (prf == null)
            {
                return NotFound();
            }

            return Ok(prf);
		}
		#endregion
	}
}
