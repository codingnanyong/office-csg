using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using System.Net.Mime;
using System.Numerics;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Master] Issue Master API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class IssueMstController : ControllerBase
	{
		#region Constructors

		private readonly IIssueMstRepo _repo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<IssueMst> _csv;
		private readonly IJsonResultService<IssueMst> _json;
		private readonly IXmlResultService<IssueMst> _xml;

        /// <summary>
        /// IssueMstController Constructor Definition
        /// </summary>
        /// <param name="repo">[Master] IssueMst Repository</param>
        /// <param name="logger">Logging Manager</param>
		/// <param name="csv">[Master] IssueMst data to CSV</param>
        /// <param name="json">[Master] IssueMst data to JSON</param>
        /// <param name="xml">[Master] IssueMst data to XML</param>
        public IssueMstController(IIssueMstRepo repo, ICsvResultService<IssueMst> csv, IJsonResultService<IssueMst> json, IXmlResultService<IssueMst> xml, ILoggerManager logger)
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
        /// All Issue Info.
        /// </summary>
        /// <response code="200">Returns a list of active issues.</response>
        /// <response code="404">If no issues are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.PCC.IssueMst"/></returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<IssueMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<IssueMst> IssueMst = _repo.GetAll() as List<IssueMst> ?? null!;

			if (IssueMst == null)
			{
                return NotFound();
            }
			
			return Ok(IssueMst);
		}

        /// <summary>
        /// (async) All Issue Info.
        /// </summary>
        /// <response code="200">Returns a list of active issues.</response>
        /// <response code="404">If no issues are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.PCC.IssueMst"/></returns>
        [HttpGet]
		[Route("async")]
        [ProducesResponseType(typeof(List<IssueMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
			List<IssueMst> IssueMst = await _repo.GetAllAsync() as List<IssueMst> ?? null!;

            if (IssueMst == null)
            {
                return NotFound();
            }

            return Ok(IssueMst);
		}

        /// <summary>
        /// All Issue Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File Format (json, csv, xml)</param>
        /// <response code="200">Returns the requested file format of active issues.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no issues are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.PCC.IssueMst"/>.</returns>
        [HttpGet("file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<IssueMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			List<IssueMst> IssueMst = _repo.GetAll() as List<IssueMst> ?? null!;

            if (IssueMst == null)
            {
                return NotFound();
            }

            string filename = "IssuMst_List";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(IssueMst, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(IssueMst, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(IssueMst, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
		}

        /// <summary>
        /// Selected Issue Info.
        /// </summary>
        /// <param name="code">Issue Code.</param>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <response code="200">Returns the selected issue information.</response>
        /// <response code="400">If the issue code is not provided.</response>
        /// <response code="404">If the specified issue is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns selected <see cref="CSG.MI.DAO.Production.PCC.IssueMst"/>.</returns>
        [HttpGet("{factory}/{code}")]
        [ProducesResponseType(typeof(IssueMst),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get([Required] string code, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(code))
            {
                return BadRequest();
            }

            IssueMst IssueMst = _repo.Get(code, factory);

			if (IssueMst == null)
            {
                return NotFound();
            }

            return Ok(IssueMst);
		}

        /// <summary>
        /// (async) Selected Issue Info.
        /// </summary>
        /// <param name="code">Issue Code.</param>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <response code="200">Returns the selected issue information.</response>
        /// <response code="400">If the issue code is not provided.</response>
        /// <response code="404">If the specified issue is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns selected <see cref="CSG.MI.DAO.Production.PCC.IssueMst"/>.</returns>
        [HttpGet("{factory}/{code}/async")]
        [ProducesResponseType(typeof(IssueMst), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync([Required] string code, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(code))
            {
                return BadRequest();
            }

            IssueMst IssueMst = await _repo.GetAsync(code, factory);

            if (IssueMst == null)
            {
                return NotFound();
            }

            return Ok(IssueMst);
		}

        /// <summary>
        /// Selected Issue Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="code">Issue Code.</param>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="format">File Format (JSON, XML, CSV) (default is JSON).</param>
        /// <response code="200">Returns the selected issue information in the specified format.</response>
        /// <response code="400">If the issue code is not provided or if the provided file format is not supported.</response>
        /// <response code="404">If the specified issue is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.PCC.IssueMst"/>.</returns>
        [HttpGet("{factory}/{code}/file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(IssueMst), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFile([Required] string code, string factory = "DS", string format = "json")
		{
			if (String.IsNullOrWhiteSpace(code))
            {
                return BadRequest();
            }

            IssueMst IssueMst = _repo.Get(code, factory);

            if (IssueMst == null)
            {
                return NotFound();
            }

            string filename = "IssuMst";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(IssueMst, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(IssueMst, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(IssueMst, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

		#endregion
	}
}
