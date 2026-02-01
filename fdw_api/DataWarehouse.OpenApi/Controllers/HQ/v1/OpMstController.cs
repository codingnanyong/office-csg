using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Master] Operation Code Master API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class OpMstController : Controller
	{
		#region Constructors

		private readonly IOpMstRepo _repo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<OpMst> _csv;
		private readonly IJsonResultService<OpMst> _json;
		private readonly IXmlResultService<OpMst> _xml;

        /// <summary>
        /// OpMstController Constructor Definition
        /// </summary>
        /// <param name="repo">[Master] Operation Repository</param>
        /// <param name="logger">Logging Manager</param>
        /// <param name="csv">[Master] Operation data to CSV</param>
        /// <param name="json">[Master] Operation data to JSON</param>
        /// <param name="xml">[Master] Operation data to XML</param>
        public OpMstController(IOpMstRepo repo, ICsvResultService<OpMst> csv, IJsonResultService<OpMst> json, IXmlResultService<OpMst> xml, ILoggerManager logger)
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
        /// All Operation Info.
        /// </summary>
        /// <response code="200">Returns a list of active Operations.</response>
        /// <response code="404">If no operations are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.PCC.OpMst"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<OpMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<OpMst> opMst = _repo.GetAll() as List<OpMst> ?? null!;

			if (opMst == null)
			{
                return NotFound();
            }

			return Ok(opMst);
		}

        /// <summary>
        /// (async) All Operation Info.
        /// </summary>
        /// <response code="200">Returns a list of active Operations.</response>
        /// <response code="404">If no operations are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DAO.Production.PCC.OpMst"/>.</returns>
        [HttpGet]
		[Route("async")]
        [ProducesResponseType(typeof(List<OpMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
			List<OpMst> opMst = await _repo.GetAllAsync() as List<OpMst> ?? null!;

            if (opMst == null)
            {
                return NotFound();
            }

            return Ok(opMst);
		}

        /// <summary>
        /// All Operation Info File(JSON,CSV,XML) Download..
        /// </summary>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns a list of active Operations.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no operations are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DAO.Production.PCC.OpMst"/></returns>
        [HttpGet("file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<OpMst>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			List<OpMst> opMst = _repo.GetAll() as List<OpMst> ?? null!;

            if (opMst == null)
            {
                return NotFound();
            }

            string filename = "OpMst_list";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(opMst, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(opMst, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(opMst, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Selected Operation Info.
        /// </summary>
        /// <param name="code">Operation Code.</param>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <response code="200">Returns the selected operation information.</response>
        /// <response code="400">If the operation code is not provided.</response>
        /// <response code="404">If the specified operation is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns the selected <see cref="CSG.MI.DAO.Production.PCC.OpMst"/>.</returns>
        [HttpGet]
		[Route("{factory}/{code}")]
        [ProducesResponseType(typeof(OpMst), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get([Required] string code, string factory = "DS")
		{
			if (String.IsNullOrEmpty(code))
            {
                return BadRequest();
            }

            OpMst opMst = _repo.Get(code, factory);

			if (opMst == null)
            {
                return NotFound();
            }

            return Ok(opMst);
		}

        /// <summary>
        /// (async) Selected Operation Info.
        /// </summary>
        /// <param name="code">Operation Code.</param>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <response code="200">Returns the selected operation information.</response>
        /// <response code="400">If the operation code is not provided.</response>
        /// <response code="404">If the specified operation is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns the selected <see cref="CSG.MI.DAO.Production.PCC.OpMst"/>.</returns>
        [HttpGet]
		[Route("{factory}/{code}/async")]
        [ProducesResponseType(typeof(OpMst), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync([Required] string code, string factory = "DS")
		{
			if (String.IsNullOrEmpty(code))
            {
                return BadRequest();
            }

            OpMst opMst = await _repo.GetAsync(code, factory);

            if (opMst == null)
            {
                return NotFound();
            }

            return Ok(opMst);
		}

        /// <summary>
        /// Selected Operation Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="code">Operation Code.</param>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the selected operation information.</response>
        /// <response code="400">If the operation code is not provided or if the provided file format is not supported.</response>
        /// <response code="404">If the specified operation is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing the selected <see cref="CSG.MI.DAO.Production.PCC.OpMst"/>.</returns>
        [HttpGet]
		[Route("{factory}/{code}/file")]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(OpMst), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFile([Required] string code, string factory = "DS", string format = "json")
		{
			if (String.IsNullOrEmpty(code))
            {
                return BadRequest();
            }

            OpMst opMst = _repo.Get(code, factory);

            if (opMst == null)
            {
                return NotFound();
            }

            string filename = "OpMst";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(opMst, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(opMst, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(opMst, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

		#endregion
	}
}
