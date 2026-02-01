using CSG.MI.DAO.Production.PCC;
using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.BLL.Production.RTLS.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Master] Process Master API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class ProcessLocMstController : ControllerBase
	{
		#region Constructors

		private readonly IProcessMstRepo _repo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<ProcessMst> _csv;
		private readonly IJsonResultService<ProcessMst> _json;
		private readonly IXmlResultService<ProcessMst> _xml;

        /// <summary>
        /// ProcessMstController Constructor Definition
        /// </summary>
        /// <param name="repo">[Master] Process Repository</param>
        /// <param name="logger">Logging Manager</param>
        /// <param name="csv">[Master] Process data to CSV</param>
        /// <param name="json">[Master] Process data to JSON</param>
        /// <param name="xml">[Master] Process data to XML</param>
        public ProcessLocMstController(IProcessMstRepo repo, ICsvResultService<ProcessMst> csv, IJsonResultService<ProcessMst> json, IXmlResultService<ProcessMst> xml, ILoggerManager logger)
		{
			_repo = repo;
			_csv = csv;
			_json = json;
			_xml = xml;
			_logger = logger;
		}

        #endregion

        #region Controllers

        /// <summary>
        /// All Processes Info.
        /// </summary>
        /// <response code="200">Returns a list of active processes.</response>
        /// <response code="404">If no processes are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active  <see cref="CSG.MI.DAO.Production.RTLS.ProcessMst"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<ProcessMst>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<ProcessMst> processMst = _repo.GetAll() as List<ProcessMst> ?? null!;

			if (processMst == null)
            {
                return NotFound();
            }

            return Ok(processMst);
		}

        /// <summary>
        /// (async) All Processes Info async Call.
        /// </summary>
        /// <response code="200">Returns a list of active processes.</response>
        /// <response code="404">If no processes are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.RTLS.ProcessMst"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<ProcessMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
			List<ProcessMst> processMst = await _repo.GetAllAsync() as List<ProcessMst> ?? null!;

            if (processMst == null)
            {
                return NotFound();
            }

            return Ok(processMst);
		}


        /// <summary>
        /// All Processes Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns a file containing a list of active processes.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no processes are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.RTLS.ProcessMst"/>.</returns>
        [HttpGet("file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<ProcessMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			List<ProcessMst> processMst = _repo.GetAll() as List<ProcessMst> ?? null!;

            if (processMst == null)
            {
                return NotFound();
            }

            string filename = "processMst_list";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(processMst, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(processMst, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(processMst, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Selected Process Info.
        /// </summary>
        /// <param name="loc">Process Location Code.</param>
        /// <response code="200">Returns the selected process information.</response>
        /// <response code="400">If the process location code is not provided.</response>
        /// <response code="404">If the specified process is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns the <see cref="CSG.MI.DAO.Production.RTLS.ProcessMst"/> for the specified location code.</returns>
        [HttpGet("{loc}")]
        [ProducesResponseType(typeof(ProcessMst),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(string loc)
		{
			if (String.IsNullOrEmpty(loc))
            {
                return BadRequest();
            }

            ProcessMst processMst = _repo.Get(loc);

			if (processMst == null)
            {
                return NotFound();
            }

            return Ok(processMst);
		}

        /// <summary>
        /// (async) Selected Process Info.
        /// </summary>
        /// <param name="loc">Process Location Code.</param>
        /// <response code="200">Returns the selected process information.</response>
        /// <response code="400">If the process location code is not provided.</response>
        /// <response code="404">If the specified process is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns the <see cref="CSG.MI.DAO.Production.RTLS.ProcessMst"/> for the specified location code.</returns>
        [HttpGet("{loc}/async")]
        [ProducesResponseType(typeof(ProcessMst), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync(string loc)
		{
			if (String.IsNullOrEmpty(loc))
            {
                return BadRequest();
            }

            ProcessMst processMst = await _repo.GetAsync(loc);

			if (processMst == null)
            {
                return NotFound();
            }

            return Ok(processMst);
		}

        /// <summary>
        /// Selected Process Info Json Download.
        /// </summary>
        /// <param name="loc">Process Location Code.</param>
        /// <param name="format">Download File Format (json, csv, xml).</param>
        /// <response code="200">Returns the selected process information in the specified file format.</response>
        /// <response code="400">If the process location code is not provided or if the provided file format is not supported.</response>
        /// <response code="404">If the specified process is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.RTLS.ProcessMst"/>.</returns>
        [HttpGet("{loc}/file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(ProcessMst), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFile(string loc, string format = "json")
		{
			if (String.IsNullOrEmpty(loc))
            {
                return BadRequest();
            }

            ProcessMst processMst = _repo.Get(loc);

			if (processMst == null)
            {
                return NotFound();
            }

            string filename = "processMst";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(processMst, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(processMst, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(processMst, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

		#endregion
	}
}
