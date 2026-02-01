using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.BLL.Production.RTLS.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Production] ERP Current Plan API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]s")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class ErpPlanController : ControllerBase
	{
		#region Constructors

		private readonly IErpPlanRepo _repo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<ErpPlan> _csv;
		private readonly IJsonResultService<ErpPlan> _json;
		private readonly IXmlResultService<ErpPlan> _xml;

        /// <summary>
        /// ErpPlanController Constructor Definition.
        /// </summary>
        /// <param name="repo">[Production] ErpPlan Repository</param>
        /// <param name="csv">[Production] ErpPlan data to CSV</param>
        /// <param name="json">[Production] ErpPlan data to JSON</param>
        /// <param name="xml">[Production] ErpPlan data to XML</param>
        /// <param name="logger">Logging Manager</param>
        public ErpPlanController(IErpPlanRepo repo, ICsvResultService<ErpPlan> csv, IJsonResultService<ErpPlan> json, IXmlResultService<ErpPlan> xml, ILoggerManager logger)
		{
			_repo = repo;
			_csv = csv;
			_json = json;
			_xml = xml;
			_logger = logger;
		}

        #endregion

        #region Controller

        /// <summary>
        /// All Tag's Erp Current plan Data Info.
        /// </summary>
        /// <response code="200">Returns a list of active current ERP plan data.</response>
        /// <response code="404">If no current plan data is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<ErpPlan>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<ErpPlan> plans = _repo.GetCurrentAll() as List<ErpPlan> ?? null!;

			if (plans == null)
			{
                return NotFound();
            }

			return Ok(plans);
		}

        /// <summary>
        /// (async) All Tag's Erp Current plan Data Info.
        /// </summary>
        /// <response code="200">Returns a list of active current ERP plan data.</response>
        /// <response code="404">If no current plan data is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<ErpPlan>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
			List<ErpPlan> plans = await _repo.GetCurrentAllAsync() as List<ErpPlan> ?? null!;

			if (plans == null)
            {
                return NotFound();
            }

            return Ok(plans);
		}

        /// <summary>
        /// All Tag's Erp Current plan Data Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File Format(JSON,XML,CSV)</param>
        /// <response code="200">Returns a list of active current ERP plan data.</response>
        /// <response code="404">If no current plan data is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<ErpPlan>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			List<ErpPlan> plans = _repo.GetCurrentAll() as List<ErpPlan> ?? null!;

			if (plans == null)
            {
                return NotFound();
            }

            string filename = "cur_plan_list";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(plans, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(plans, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(plans, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
		}

        /// <summary>
        /// Selected Tag's Erp Current plan Data Info.
        /// </summary>
        /// <param name="tagid">BeaconTag Id.</param>
        /// <response code="200">Returns the current ERP plan data for the specified tag.</response>
        /// <response code="400">If the request is invalid or missing required parameters.</response>
        /// <response code="404">If no current plan data is found for the specified tag.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns the current <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("{tagid}")]
        [ProducesResponseType(typeof(ErpPlan),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(string tagid)
		{
			if (String.IsNullOrEmpty(tagid))
            {
                return BadRequest();
            }

            ErpPlan plan = _repo.GetCurrent(tagid);

			if (plan == null)
            {
                return NotFound();
            }

            return Ok(plan);
		}

        /// <summary>
        /// (async) Selected Tag's Erp Current plan Data Info.
        /// </summary>
        /// <param name="tagid">BeaconTag Id.</param>
        /// <response code="200">Returns the current ERP plan data for the specified tag.</response>
        /// <response code="400">If the request is invalid or missing required parameters.</response>
        /// <response code="404">If no current plan data is found for the specified tag.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns the current <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("{tagid}/async")]
        [ProducesResponseType(typeof(ErpPlan), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync(string tagid)
		{
			if (String.IsNullOrEmpty(tagid))
            {
                return BadRequest();
            }

            ErpPlan plan = await _repo.GetCurrentAsync(tagid);

			if (plan == null)
            {
                return NotFound();
            }

            return Ok(plan);
		}

        /// <summary>
        /// Selected Tag's Erp Current plan Data Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="tagid">BeaconTag Id.</param>
        /// <param name="format">File Format(JSON,XML,CSV)</param>
        /// <response code="200">Returns the current ERP plan data for the specified tag.</response>
        /// <response code="400">If the request is invalid or missing required parameters or if the provided file format is not supported.</response>
        /// <response code="404">If no current plan data is found for the specified tag.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("{tagid}/file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFile(string tagid, string format = "json")
		{
			if (String.IsNullOrEmpty(tagid))
            {
                return BadRequest();
            }

            ErpPlan plan = _repo.GetCurrent(tagid);

			if (plan == null)
            {
                return NotFound();
            }

            string filename = "slc_cur_plan";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(plan, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(plan, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(plan, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Searched Erp Plan Data List Info.
        /// </summary>
        /// <param name="wsno">WorkSheet Number.</param>
        /// <param name="strPblDt">Work order production date (From).</param>
        /// <param name="endPblDt">Work order production date (To).</param>
        /// <param name="pm">Project Manager Name.</param>
        /// <param name="season">Season Code.</param>
        /// <param name="ctg">Category.</param>
        /// <param name="model">Model Name.</param>
        /// <param name="stcd">Round Code.</param>
        /// <param name="clr">BOM Id / Dev Colorway Id.</param>
        /// <param name="size">Size Code.</param>
        /// <param name="gnd">Gender.</param>
        /// <param name="strdueDt">Delivery date inquiry (From).</param>
        /// <param name="enddueDt">Delivery date inquiry (To).</param>
        /// <param name="strplnDt">Production completion Plan date (From).</param>
        /// <param name="endplnDt">Production completion Plan date (To).</param>
        /// <param name="prdfc">Factory to produce Code.</param>
        /// <param name="pst">PassType.</param>
        /// <param name="tagid">Tag Id.</param>
        /// <response code="200">Returns a list of ERP plan data based on the search criteria.</response>
        /// <response code="400">If the request is invalid or missing required parameters.</response>
        /// <response code="404">If no ERP plan data is found based on the search criteria.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("search")]
        [ProducesResponseType(typeof(List<ErpPlan>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSearch(string? wsno,
									   [RegularExpression("^\\d{4}-\\d{2}-\\d{2}$", ErrorMessage = "The format of the date must be 'yyyy-MM-dd'.")] string? strPblDt,
									   [RegularExpression("^\\d{4}-\\d{2}-\\d{2}$", ErrorMessage = "The format of the date must be 'yyyy-MM-dd'.")] string? endPblDt,
									   string? pm, string? season, string? ctg, string? model, string? stcd, string? clr, string? size, string? gnd,
									   string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid)
		{
			if (String.IsNullOrEmpty(wsno) || String.IsNullOrEmpty(strPblDt) || String.IsNullOrEmpty(endPblDt) || String.IsNullOrEmpty(pm) || String.IsNullOrEmpty(season) ||
				String.IsNullOrEmpty(ctg) || String.IsNullOrEmpty(model) || String.IsNullOrEmpty(stcd) || String.IsNullOrEmpty(clr) ||
				String.IsNullOrEmpty(size) || String.IsNullOrEmpty(gnd) || String.IsNullOrEmpty(prdfc) || String.IsNullOrEmpty(pst) ||
				String.IsNullOrEmpty(tagid) || String.IsNullOrEmpty(strdueDt) || String.IsNullOrEmpty(enddueDt) || String.IsNullOrEmpty(strplnDt) || String.IsNullOrEmpty(endplnDt))
				return BadRequest();

			List<ErpPlan> plan =  _repo.GetSearch(wsno, strPblDt, endPblDt, pm, season, ctg, model, stcd, clr, size, gnd, strdueDt, enddueDt, prdfc, strplnDt, endplnDt, pst, tagid) as List<ErpPlan> ?? new List<ErpPlan>();

			if (plan == null)
            {
                return NotFound();
            }

            return Ok(plan);
		}

        /// <summary>
        /// (async) Searched Erp Plan Data List Info.
        /// </summary>
        /// <param name="wsno">WorkSheet Number.</param>
        /// <param name="strPblDt">Work order production date (From).</param>
        /// <param name="endPblDt">Work order production date (To).</param>
        /// <param name="pm">Project Manager Name.</param>
        /// <param name="season">Season Code.</param>
        /// <param name="ctg">Category.</param>
        /// <param name="model">Model Name.</param>
        /// <param name="stcd">Round Code.</param>
        /// <param name="clr">BOM Id / Dev Colorway Id.</param>
        /// <param name="size">Size Code.</param>
        /// <param name="gnd">Gender.</param>
        /// <param name="strdueDt">Delivery date inquiry (From).</param>
        /// <param name="enddueDt">Delivery date inquiry (To).</param>
        /// <param name="strplnDt">Production completion Plan date (From).</param>
        /// <param name="endplnDt">Production completion Plan date (To).</param>
        /// <param name="prdfc">Factory to produce Code.</param>
        /// <param name="pst">PassType.</param>
        /// <param name="tagid">Tag Id.</param>
        /// <response code="200">Returns a list of ERP plan data based on the search criteria.</response>
        /// <response code="400">If the request is invalid or missing required parameters.</response>
        /// <response code="404">If no ERP plan data is found based on the search criteria.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("async/search")]
        [ProducesResponseType(typeof(List<ErpPlan>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetSearchAsync(string? wsno,
													   [RegularExpression("^\\d{4}-\\d{2}-\\d{2}$", ErrorMessage = "The format of the date must be 'yyyy-MM-dd'.")] string? strPblDt,
													   [RegularExpression("^\\d{4}-\\d{2}-\\d{2}$", ErrorMessage = "The format of the date must be 'yyyy-MM-dd'.")] string? endPblDt,
													   string? pm, string? season, string? ctg, string? model, string? stcd, string? clr, string? size, string? gnd,
													   string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid)
		{
			if (String.IsNullOrEmpty(wsno) || String.IsNullOrEmpty(strPblDt) || String.IsNullOrEmpty(endPblDt) || String.IsNullOrEmpty(pm) || String.IsNullOrEmpty(season) ||
				String.IsNullOrEmpty(ctg) || String.IsNullOrEmpty(model) || String.IsNullOrEmpty(stcd) || String.IsNullOrEmpty(clr) ||
				String.IsNullOrEmpty(size) || String.IsNullOrEmpty(gnd) || String.IsNullOrEmpty(prdfc) || String.IsNullOrEmpty(pst) ||
				String.IsNullOrEmpty(tagid) || String.IsNullOrEmpty(strdueDt) || String.IsNullOrEmpty(enddueDt) || String.IsNullOrEmpty(strplnDt) || String.IsNullOrEmpty(endplnDt))
				return BadRequest();

			List<ErpPlan> plan = await _repo.GetSearchAsync(wsno, strPblDt, endPblDt, pm, season, ctg, model, stcd, clr, size, gnd, strdueDt, enddueDt, prdfc, strplnDt, endplnDt, pst, tagid) as List<ErpPlan> ?? new List<ErpPlan>();

			if (plan == null)
            {
                return NotFound();
            }

            return Ok(plan);
		}

        /// <summary>
        /// Searched Erp Plan Data List Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="wsno">WorkSheet Number.</param>
        /// <param name="strPblDt">Work order production date (From).</param>
        /// <param name="endPblDt">Work order production date (To).</param>
        /// <param name="pm">Project Manager Name.</param>
        /// <param name="season">Season Code.</param>
        /// <param name="ctg">Category.</param>
        /// <param name="model">Model Name.</param>
        /// <param name="stcd">Round Code.</param>
        /// <param name="clr">BOM Id / Dev Colorway Id.</param>
        /// <param name="size">Size Code.</param>
        /// <param name="gnd">Gender.</param>
        /// <param name="strdueDt">Delivery date inquiry (From).</param>
        /// <param name="enddueDt">Delivery date inquiry (To).</param>
        /// <param name="strplnDt">Production completion Plan date (From).</param>
        /// <param name="endplnDt">Production completion Plan date (To).</param>
        /// <param name="prdfc">Factory to produce Code.</param>
        /// <param name="pst">PassType.</param>
        /// <param name="tagid">Tag Id.</param>
        /// <param name="format">File Format(JSON,XML,CSV)</param>
        /// <response code="200">Returns a list of ERP plan data based on the search criteria.</response>
        /// <response code="400">If the request is invalid or missing required parameters or if the provided file format is not supported.</response>
        /// <response code="404">If no ERP plan data is found based on the search criteria.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.RTLS.ErpPlan"/>.</returns>
        [HttpGet("search/file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<ErpPlan>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSearchFile(string? wsno,
									   [RegularExpression("^\\d{4}-\\d{2}-\\d{2}$", ErrorMessage = "The format of the date must be 'yyyy-MM-dd'.")] string? strPblDt,
									   [RegularExpression("^\\d{4}-\\d{2}-\\d{2}$", ErrorMessage = "The format of the date must be 'yyyy-MM-dd'.")] string? endPblDt,
									   string? pm, string? season, string? ctg, string? model, string? stcd, string? clr, string? size, string? gnd,
									   string? strdueDt, string? enddueDt, string? strplnDt, string? endplnDt, string? prdfc, string? pst, string? tagid,
									   string format = "json")
		{
			if (String.IsNullOrEmpty(wsno) || String.IsNullOrEmpty(strPblDt) || String.IsNullOrEmpty(endPblDt) || String.IsNullOrEmpty(pm) || String.IsNullOrEmpty(season) ||
				String.IsNullOrEmpty(ctg) || String.IsNullOrEmpty(model) || String.IsNullOrEmpty(stcd) || String.IsNullOrEmpty(clr) ||
				String.IsNullOrEmpty(size) || String.IsNullOrEmpty(gnd) || String.IsNullOrEmpty(prdfc) || String.IsNullOrEmpty(pst) ||
				String.IsNullOrEmpty(tagid) || String.IsNullOrEmpty(strdueDt) || String.IsNullOrEmpty(enddueDt) || String.IsNullOrEmpty(strplnDt) || String.IsNullOrEmpty(endplnDt))
				return BadRequest();

			List<ErpPlan> plan = _repo.GetSearch(wsno, strPblDt, endPblDt, pm, season, ctg, model, stcd, clr, size, gnd, strdueDt, enddueDt, prdfc, strplnDt, endplnDt, pst, tagid) as List<ErpPlan> ?? new List<ErpPlan>();

			if (plan == null)
            {
                return NotFound();
            }

            string filename = "srch_plan_list";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(plan, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(plan, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(plan, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
		}
		#endregion
	}
}
