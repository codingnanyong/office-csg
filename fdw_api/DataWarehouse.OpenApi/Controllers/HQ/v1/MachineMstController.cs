using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Master] Machine Master API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class MachineMstController : ControllerBase
	{
		#region Constructors

		private readonly IMachineMstRepo _repo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<MachineMst> _csv;
		private readonly IJsonResultService<MachineMst> _json;
		private readonly IXmlResultService<MachineMst> _xml;

        /// <summary>
        /// MachineMstController Constructor Definition
        /// </summary>
        /// <param name="repo">[Master] Machine Repository</param>
        /// <param name="logger">Logging Manager</param>
        /// <param name="csv">[Master] Machine data to CSV</param>
        /// <param name="json">[Master] Machine data to JSON</param>
        /// <param name="xml">[Master] Machine data to XML</param>
        public MachineMstController(IMachineMstRepo repo, ICsvResultService<MachineMst> csv, IJsonResultService<MachineMst> json, IXmlResultService<MachineMst> xml, ILoggerManager logger)
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
        /// All Machine Info.
        /// </summary>
        /// <response code="200">Returns a list of active Machines.</response>
        /// <response code="404">If no machines are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.PCC.MachineMst"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<MachineMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<MachineMst> MachineMst = _repo.GetAll() as List<MachineMst> ?? null!;

			if (MachineMst == null)
			{
                return NotFound();
            }

			return Ok(MachineMst);
		}

        /// <summary>
        /// (async) All Machine Info.
        /// </summary>
        /// <response code="200">Returns a list of active Machines.</response>
        /// <response code="404">If no machines are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.PCC.MachineMst"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<MachineMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
			List<MachineMst> MachineMst = await _repo.GetAllAsync() as List<MachineMst> ?? null!;

            if (MachineMst == null)
            {
                return NotFound();
            }

            return Ok(MachineMst);
		}

        /// <summary>
        /// All Machine Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the file containing active Machines.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no machines are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="DAO.Production.PCC.MachineMst"/>.</returns>
        [HttpGet("file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<MachineMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			if (String.IsNullOrEmpty(format))
			{
                return BadRequest();
            }

			List<MachineMst> MachineMst = _repo.GetAll() as List<MachineMst> ?? null!;

            if (MachineMst == null)
            {
                return NotFound();
            }

            string filename = "Machine_list";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(MachineMst, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(MachineMst, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(MachineMst, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

		#endregion
	}
}
