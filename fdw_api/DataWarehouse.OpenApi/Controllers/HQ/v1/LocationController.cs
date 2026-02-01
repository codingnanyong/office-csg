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
    /// [Production] Tag's Current Position API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class LocationController : ControllerBase
	{
		#region Constructors

		private readonly IEslLocationRepo _repo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<EslLocation> _csv;
		private readonly IJsonResultService<EslLocation> _json;
		private readonly IXmlResultService<EslLocation> _xml;

        /// <summary>
        /// LocationController Constructor Definition.
        /// </summary>
        /// <param name="repo">[Production] Location Repository</param>
        /// <param name="logger">Logging Manager</param>
        /// <param name="csv">[Production] Location data to CSv</param>
        /// <param name="json">[Production] Location data to JSON</param>
        /// <param name="xml">[Production] Location data to XML</param>
        public LocationController(IEslLocationRepo repo, ICsvResultService<EslLocation> csv, IJsonResultService<EslLocation> json, IXmlResultService<EslLocation> xml, ILoggerManager logger)
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
        /// All Tag's current position Info.
        /// </summary>
        /// <response code="200">Returns a list of active Locations.</response>
        /// <response code="404">If no tag positions are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.RTLS.EslLocation"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<EslLocation>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<EslLocation> locations = _repo.GetCurrentAll() as List<EslLocation> ?? null!;

			if (locations == null)
            {
                return NotFound();
            }

            return Ok(locations);
		}

        /// <summary>
        /// (async) All Tag's current position Info.
        /// </summary>
        /// <response code="200">Returns a list of active Locations.</response>
        /// <response code="404">If no tag positions are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.RTLS.EslLocation"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<EslLocation>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
            List<EslLocation> locations = await _repo.GetCurrentAllAsync() as List<EslLocation> ?? null!;

            if (locations == null)
            {
                return NotFound();  
            }

            return Ok(locations);
        }

        /// <summary>
        /// All Tag's current position Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File Format(JSON,XML,CSV)</param>
        /// <response code="200">Returns a list of active Locations.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no tag positions are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.RTLS.EslLocation"/>.</returns>
        [HttpGet("file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<EslLocation>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			List<EslLocation> locations = _repo.GetCurrentAll() as List<EslLocation> ?? null!;

            if (locations == null)
            {
                return NotFound();
            }

            string filename = "all_cur_loc";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(locations, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(locations, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(locations, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Selected Tag's current position Info.
        /// </summary>
        /// <param name="tagid">BeaconTag Id.</param>
        /// <response code="200">Returns active Locations.</response>
        /// <response code="400">If the tag id is not provided.</response>
        /// <response code="404">If the specified tag's position is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns active <see cref="CSG.MI.DAO.Production.RTLS.EslLocation"/>.</returns>
        [HttpGet("{tagid}")]
        [ProducesResponseType(typeof(EslLocation), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(string tagid)
		{
			if (String.IsNullOrEmpty(tagid))
            {
                return BadRequest();
            }

            EslLocation location = _repo.GetCurrent(tagid);

			if (location == null)
            {
                return NotFound();
            }

            return Ok(location);
		}

        /// <summary>
        /// (async) Selected Tag's current position Info.
        /// </summary>
        /// <param name="tagid">BeaconTag Id.</param>
        /// <response code="200">Returns active Locations.</response>
        /// <response code="400">If the tag id is not provided.</response>
        /// <response code="404">If the specified tag's position is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns active <see cref="CSG.MI.DAO.Production.RTLS.EslLocation"/>.</returns>
        [HttpGet("{tagid}/async")]
        [ProducesResponseType(typeof(EslLocation), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync(string tagid)
		{
			if (String.IsNullOrEmpty(tagid))
            {
                return BadRequest();
            }

            EslLocation location = await _repo.GetCurrentAsync(tagid);

            if (location == null)
            {
                return NotFound();
            }

            return Ok(location);
		}

        /// <summary>
        /// Selected Tag's current position Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="tagid">BeaconTag Id.</param>
        /// <param name="format">File Format(JSON,XML,CSV)</param>
        /// <response code="200">Returns active Locations.</response>
        /// <response code="400">If the tag id is not provided or if the provided file format is not supported.</response>
        /// <response code="404">If the specified tag's position is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.RTLS.EslLocation"/>.</returns>
        [HttpGet("{tagid}/file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(EslLocation), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFile(string tagid, string format = "json")
		{
			if (String.IsNullOrEmpty(tagid))
            {
                return BadRequest();
            }

            EslLocation location = _repo.GetCurrent(tagid);

            if (location == null)
			{
				return NotFound();
			}

            string filename = "tag_cur_loc";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(location, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(location, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(location, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }
		#endregion
	}
}
