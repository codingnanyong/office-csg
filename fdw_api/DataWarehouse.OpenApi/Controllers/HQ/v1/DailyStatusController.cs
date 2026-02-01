using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Production] Daily Production Status API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class DailyStatusController : ControllerBase
	{
		#region Constructors

		private readonly IDailyStatusRepo _repo;
		private readonly ILoggerManager _logger;

		private readonly ICsvResultService<DailyStatus> _dailyCsv;
		private readonly IJsonResultService<DailyStatus> _dailyJson;
		private readonly IXmlResultService<DailyStatus> _dailyXml;

		private readonly ICsvResultService<SampleWork> _workCsv;
		private readonly IJsonResultService<SampleWork> _workJson;
		private readonly IXmlResultService<SampleWork> _workXml;

        /// <summary>
        /// DailyProductionController Constructor Definition
        /// </summary>
        /// <param name="repo">[Production] DailyStatus Repository</param>
        /// <param name="logger">Logging Manager</param>
        /// <param name="dailyCsv">[Production] DailyStatus data CSV</param>
        /// <param name="dailyJson">[Production] DailyStatus data JSON</param>
        /// <param name="dailyXml">[Production] DailyStatus data XML</param>
        /// <param name="workCsv">[Production] WorkList data to CSV</param>
        /// <param name="workJson">[Production] WorkList data to JSON</param>
        /// <param name="workXml">[Production] WorkList data to XML</param>
        public DailyStatusController(IDailyStatusRepo repo, ILoggerManager logger,
									 ICsvResultService<DailyStatus> dailyCsv, IJsonResultService<DailyStatus> dailyJson, IXmlResultService<DailyStatus> dailyXml,
                                     ICsvResultService<SampleWork> workCsv, IJsonResultService<SampleWork> workJson, IXmlResultService<SampleWork> workXml)
        {
			_repo = repo;
			_logger = logger;
			_dailyCsv = dailyCsv;
			_dailyJson = dailyJson;
			_dailyXml = dailyXml;
			_workCsv = workCsv;
			_workJson = workJson;
			_workXml = workXml;
		}

        #endregion

        #region Controllers - DailyStatus

        /// <summary>
        /// All Daily Production Status Info.
        /// </summary>
        /// <response code="200">Returns a list of active daily production statuses.</response>
        /// <response code="404">If no daily production statuses are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<DailyStatus>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<DailyStatus> dailystatus = _repo.GetDailyStatus() as List<DailyStatus> ?? null!;

			if (dailystatus == null)
            {
                return NotFound();
            }

            return Ok(dailystatus);
		}

        /// <summary>
        /// (async) All Daily Production Status Info.
        /// </summary>
        /// <response code="200">Returns a list of active daily production statuses.</response>
        /// <response code="404">If no daily production statuses are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<DailyStatus>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
			List<DailyStatus> dailystatus = await _repo.GetDailyStatusAsync() as List<DailyStatus> ?? null!;

            if (dailystatus == null)
            {
                return NotFound();
            }

            return Ok(dailystatus);
		}

        /// <summary>
        /// All Daily Production Status Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the requested file containing daily production statuses.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no daily production statuses are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("file")]
        [ProducesResponseType(typeof(List<DailyStatus>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			List<DailyStatus> dailystatus = _repo.GetDailyStatus() as List<DailyStatus> ?? null!;

            if (dailystatus == null)
            {
                return NotFound();
            }

            if (format.ToLower() == "json")
			{
				var stream = _dailyJson.Download(dailystatus, "dailystatus.json");
				return File(stream, "application/json", "dailystatus.json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _dailyCsv.Download(dailystatus, "dailystatus.csv");
				return File(stream, "text/csv", "dailystatus.csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _dailyXml.Download(dailystatus, "dailystatus.xml");
				return File(stream, "application/xml", "dailystatus.xml"); ;
			}
			else
            {
                return BadRequest();
            }
		}


        /// <summary>
        /// Daily Production Status Info By Factory.
        /// </summary>
        /// <param name="factory">Factory code.</param>
        /// <response code="200">Returns a list of active daily production statuses.</response>
        /// <response code="404">If no daily production statuses are found for the specified factory.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("{factory}")]
        [ProducesResponseType(typeof(List<DailyStatus>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetByFactory(string factory = "DS")
		{
			List<DailyStatus> dailystatus = _repo.GetDailyStatusByFactory(factory) as List<DailyStatus> ?? null!;

			if (dailystatus == null)
            {
                return NotFound();
            }

            return Ok(dailystatus);
		}

        /// <summary>
        /// (async) Daily Production Status Info By Factory.
        /// </summary>
        /// <param name="factory">Factory code.</param>
        /// <response code="200">Returns a list of active daily production statuses.</response>
        /// <response code="404">If no daily production statuses are found for the specified factory.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("{factory}/async")]
        [ProducesResponseType(typeof(List<DailyStatus>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetByFactoryAsync(string factory = "DS")
		{
			List<DailyStatus> dailystatus = await _repo.GetDailyStatusByFactoryAsync(factory) as List<DailyStatus> ?? null!;

			if (dailystatus == null)
            {
                return NotFound();
            }

            return Ok(dailystatus);
		}

        /// <summary>
        /// Daily Production Status Info By Factory File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="factory">Factory code.</param>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the requested file containing daily production statuses.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no daily production statuses are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("{factory}/file")]
        [ProducesResponseType(typeof(List<DailyStatus>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetByFactoryFile(string factory = "DS", string format = "json")
		{
			List<DailyStatus> dailystatus = _repo.GetDailyStatusByFactory(factory) as List<DailyStatus> ?? null!;

			if (dailystatus == null)
            {
                return NotFound();
            }

            string filename = factory.ToLower() + "_dailystatus";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _dailyJson.Download(dailystatus, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _dailyCsv.Download(dailystatus, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _dailyXml.Download(dailystatus, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Daily Production Status Info By Factory,Operation.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="opcd">Operation Code.</param>
        /// <response code="200">Returns the daily production status for the specified factory and operation code.</response>
        /// <response code="400">If the operation code is not provided.</response>
        /// <response code="404">If no daily production status is found for the specified factory and operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("{factory}/{opcd}")]
        [ProducesResponseType(typeof(DailyStatus), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(string opcd, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(opcd))
            {
                return BadRequest();
            }

            DailyStatus dailystatus = _repo.GetDailyStatus(opcd, factory);

			if (dailystatus == null)
            {
                return NotFound();
            }

            return Ok(dailystatus);
		}

        /// <summary>
        /// (async) Daily Production Status Info By Factory,Operation.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="opcd">Operation Code.</param>
        /// <response code="200">Returns the daily production status for the specified factory and operation code.</response>
        /// <response code="400">If the operation code is not provided.</response>
        /// <response code="404">If no daily production status is found for the specified factory and operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("{factory}/{opcd}/async")]
        [ProducesResponseType(typeof(DailyStatus), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync(string opcd, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(opcd))
            {
                return BadRequest();
            }

            DailyStatus dailystatus = await _repo.GetDailyStatusAsync(opcd, factory);

            if (dailystatus == null)
            {
                return NotFound();
            }

            return Ok(dailystatus);

		}

        /// <summary>
        /// Daily Production Status Info By Factory,Operation File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the daily production status for the specified factory and operation code.</response>
        /// <response code="400">If the operation code is not provided or if the provided file format is not supported.</response>
        /// <response code="404">If no daily production status is found for the specified factory and operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.DailyStatus"/>.</returns>
        [HttpGet("{factory}/{opcd}/file")]
        [ProducesResponseType(typeof(DailyStatus), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFile(string opcd, string factory = "DS", string format = "json")
		{
			if (String.IsNullOrWhiteSpace(opcd))
            {
                return BadRequest();
            }

            DailyStatus dailystatus = _repo.GetDailyStatus(opcd, factory);

            if (dailystatus == null)
            {
                return NotFound();
            }

            string filename = factory.ToLower() + "_" + opcd.ToLower() + "_dailystatus";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _dailyJson.Download(dailystatus, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _dailyCsv.Download(dailystatus, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _dailyXml.Download(dailystatus, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
          
        }

        #endregion

        #region Controllers - DailyStatus/Pln-Prf WorkList

        /// <summary>
        /// All Daily Pln-Prf Info.
        /// </summary>
        /// <response code="200">Returns a list of active daily planned and performed work.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkList()
		{
			List<SampleWork> worklist = _repo.GetWorkList() as List<SampleWork> ?? null!;

			if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
		}

        /// <summary>
        /// (async) All Daily Pln-Prf Info.
        /// </summary>
        /// <response code="200">Returns a list of active daily planned and performed work.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/async")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetWorkListAsync()
		{
			List<SampleWork> worklist = await _repo.GetWorkListAsync() as List<SampleWork> ?? null!;

			if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
		}

        /// <summary>
        /// All Daily Pln-Prf Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File Format (JSON, CSV, XML).</param>
        /// <response code="200">Returns a list of active daily planned and performed work.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/file")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkListFile(string format = "json")
		{
			List<SampleWork> worklist = _repo.GetWorkList() as List<SampleWork> ?? null!;

			if (worklist == null)
            {
                return NotFound();
            }

            string filename = "dailystatus_worklist";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _workJson.Download(worklist, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _workCsv.Download(worklist, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _workXml.Download(worklist, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Daily Pln-Prf Info by Factory,Operation.
        /// </summary>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="factory">Factory Code.</param>
        /// <response code="200">Returns a list of active daily planned and performed work.</response>
        /// <response code="400">If the operation code is not provided.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/{factory}/{opcd}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkListBy(string opcd, string factory = "DS")
		{
			if (string.IsNullOrEmpty(opcd))
            {
                return BadRequest();
            }

            List<SampleWork> worklist = _repo.GetWorkList(opcd, factory) as List<SampleWork> ?? null!;

            if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
		}

        /// <summary>
        /// (async) Daily Pln-Prf Info by Factory,Operation.
        /// </summary>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="factory">Factory Code.</param>
        /// <response code="200">Returns a list of active daily planned and performed work.</response>
        /// <response code="400">If the operation code is not provided.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/{factory}/{opcd}/async")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetWorkListByAsync(string opcd, string factory = "DS")
		{
			if (string.IsNullOrEmpty(opcd))
            {
                return BadRequest();
            }

            List<SampleWork> worklist = await _repo.GetWorkListAsync(opcd, factory) as List<SampleWork> ?? null!;

			if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
		}

        /// <summary>
        /// Daily Pln-Prf Info by Factory,Operation File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="factory">Factory Code.</param>
        /// <param name="format">File Format (JSON, CSV, XML).</param>
        /// <response code="200">Returns a list of active daily planned and performed work.</response>
        /// <response code="400">If the operation code is not provided or if the provided file format is not supported.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/{factory}/{opcd}/file")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkListByFile(string opcd, string factory = "DS", string format = "json")
		{
			if (string.IsNullOrEmpty(opcd))
            {
                return BadRequest();
            }

            List<SampleWork> worklist = _repo.GetWorkList(opcd, factory) as List<SampleWork> ?? null!;

			if (worklist == null)
            {
                return NotFound();
            }

            string filename = "dailystatus_worklist" + opcd.ToLower() + "_list";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _workJson.Download(worklist, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _workCsv.Download(worklist, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _workXml.Download(worklist, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Daily Pln-Prf Info Keyword Search.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="keyword">Keyword to search for.</param>
        /// <response code="200">Returns a list of active daily planned and performed work matching the keyword.</response>
        /// <response code="400">If both factory code and operation code are not provided.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/{factory}/{opcd}/keyword")]
        [ProducesResponseType(typeof(List<SampleWork>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSearch(string opcd, string? keyword, string factory = "DS")
		{
			if (string.IsNullOrEmpty(factory) & string.IsNullOrEmpty(opcd))
            {
                return BadRequest();
            }

            List<SampleWork> worklist = _repo.GetSearch(opcd, keyword, factory) as List<SampleWork> ?? null!;

			if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
		}

        /// <summary>
        /// (async) Daily Pln-Prf Info Keyword Search.
        /// </summary>
        /// <param name="factory">Factory Code.</param>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="keyword">Keyword to search for.</param>
        /// <response code="200">Returns a list of active daily planned and performed work matching the keyword.</response>
        /// <response code="400">If both factory code and operation code are not provided.</response>
        /// <response code="404">If no daily planned and performed work information is found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/{factory}/{opcd}/keyword/async")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetSearchAsync(string opcd, string? keyword, string factory = "DS")
		{
			if (string.IsNullOrEmpty(factory) & string.IsNullOrEmpty(opcd))
			{
                return BadRequest();
            }

			List<SampleWork> worklist = await _repo.GetSearchAsync(opcd, keyword, factory) as List<SampleWork> ?? null!;

			if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
		}

		#endregion
	}
}
