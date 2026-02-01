using CSG.MI.DAO.Production.PCC;
using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Production] WIP(Work In Process) API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class WipController : ControllerBase
	{
		#region Constructors

		private readonly IWsWipRepo _wipRepo;
		private readonly IWsWipRateRepo _rateRepo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<Wip> _wipCsv;
		private readonly IJsonResultService<Wip> _wipJson;
		private readonly IXmlResultService<Wip> _wipXml;

		private readonly ICsvResultService<SampleWork> _workCsv;
		private readonly IJsonResultService<SampleWork> _workJson;
		private readonly IXmlResultService<SampleWork> _workXml;

        /// <summary>
        /// WipController Constructor Definition
        /// </summary>
        /// <param name="wipRepo">Wip Repository</param>
		/// <param name="rateRepo">Wip Rate Repository</param>
        /// <param name="logger">Logging Manager</param>
        /// <param name="wipCsv">Wip data to CSV </param>
        /// <param name="wipJson">Wup data to JSON</param>
        /// <param name="wipXml">Wip data to XML</param>
        /// <param name="workCsv">WorkList data to CSV</param>
        /// <param name="workJson">WorkList data to JSON</param>
        /// <param name="workXml">WorkList data to XML</param>
        public WipController(IWsWipRepo wipRepo, IWsWipRateRepo rateRepo,ILoggerManager logger, 
							 ICsvResultService<Wip> wipCsv, IJsonResultService<Wip> wipJson, IXmlResultService<Wip> wipXml,
						     ICsvResultService<SampleWork> workCsv, IJsonResultService<SampleWork> workJson, IXmlResultService<SampleWork> workXml)
		{
			_wipRepo = wipRepo;
			_rateRepo = rateRepo;
            _logger = logger;
            _wipCsv = wipCsv;
            _wipJson = wipJson;
            _wipXml = wipXml;
            _workCsv = workCsv;
            _workJson = workJson;
            _workXml = workXml;
		}

        #endregion

        #region Controllers - WIP

        /// <summary>
        /// All Wip Info.
        /// </summary>
        /// <response code="200">Returns a list of active WIP.</response>
        /// <response code="404">If no WIP records are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Wip"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<Wip>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll()
		{
			List<Wip> wips = _wipRepo.GetAll() as List<Wip> ?? null!;

			if (wips == null)
            {
                return NotFound();
            }

            return Ok(wips);
		}

        /// <summary>
        /// (async) All Wip Info.
        /// </summary>
        /// <response code="200">Returns a list of active WIP.</response>
        /// <response code="404">If no WIP records are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Wip"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<Wip>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync()
		{
			List<Wip> wips = await _wipRepo.GetAllAsync() as List<Wip> ?? null!;

			if (wips == null)
            {
                return NotFound();
            }

            return Ok(wips);

		}

        /// <summary>
        /// All Wip Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns a list of active WIP.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no WIP records are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Wip"/>.</returns>
        /// <returns>List of active<see cref="CSG.MI.DTO.Production.Wip"/></returns>
        [HttpGet("file")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<Wip>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAllFile(string format = "json")
		{
			List<Wip> wips = _wipRepo.GetAll() as List<Wip> ?? null!;

			if (wips == null)
            {
                return NotFound();
            }

            string filename = "wip_list";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _wipJson.Download(wips, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _wipCsv.Download(wips, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _wipXml.Download(wips, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Wip By Factory Info.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <response code="200">Returns a list of active WIP for the specified factory.</response>
        /// <response code="404">If no WIP records are found for the specified factory.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active<see cref="CSG.MI.DTO.Production.Wip"/></returns>
        [HttpGet("{factory}")]
        [ProducesResponseType(typeof(List<Wip>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetByFactory(string factory = "DS")
		{
			List<Wip> wips = _wipRepo.GetByFactory(factory) as List<Wip> ?? null!;

			if (wips == null)
            {
                return NotFound();
            }

            return Ok(wips);
		}

        /// <summary>
        /// (async) Wip By Factory Info.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <response code="200">Returns a list of active WIP for the specified factory.</response>
        /// <response code="404">If no WIP records are found for the specified factory.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active<see cref="CSG.MI.DTO.Production.Wip"/></returns>
        [HttpGet("{factory}/async")]
        [ProducesResponseType(typeof(List<Wip>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetByFactoryAsync(string factory = "DS")
		{
			List<Wip> wips = await _wipRepo.GetByFactoryAsync() as List<Wip> ?? null!;

			if (wips == null)
            {
                return NotFound();
            }

            return Ok(wips);
		}

        /// <summary>
        /// Wip By Factory Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns a file containing active WIP for the specified factory.</response>
        /// <response code="400">If the file format is not supported.</response>
        /// <response code="404">If no WIP records are found for the specified factory.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.Wip"/></returns>
        [HttpGet("{factory}/file")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<Wip>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetByFactoryFile(string factory = "DS", string format = "json")
		{
			List<Wip> wips = _wipRepo.GetByFactory(factory) as List<Wip> ?? null!;

			if (wips == null)
            {
                return NotFound();
            }

            string filename = $"wip_list_{factory.ToLower()}";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _wipJson.Download(wips, $"{filename}.json");
                    return File(jsonStream, "application/json", $"{filename}.json");

                case "csv":
                    var csvStream = _wipCsv.Download(wips, $"{filename}.csv");
                    return File(csvStream, "text/csv", $"{filename}.csv");

                case "xml":
                    var xmlStream = _wipXml.Download(wips, $"{filename}.xml");
                    return File(xmlStream, "application/xml", $"{filename}.xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Wip Info by Factory,Operation.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code.</param>
        /// <response code="200">Returns the WIP information for the specified factory and operation code.</response>
        /// <response code="400">If the operation code is missing or empty.</response>
        /// <response code="404">If no WIP records are found for the specified factory and operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.Wip"/>.</returns>
        [HttpGet("{factory}/{opcd}")]
        [ProducesResponseType(typeof(Wip), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(string opcd, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(opcd))
            {
                return BadRequest();
            }

			Wip wip = _wipRepo.GetWip(opcd, factory);

			if (wip == null)
            {
                return NotFound();
            }

            return Ok(wip);
		}

        /// <summary>
        /// (async) Wip Info by Factory,Operation.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code.</param>
        /// <response code="200">Returns the WIP information for the specified factory and operation code.</response>
        /// <response code="400">If the operation code is missing or empty.</response>
        /// <response code="404">If no WIP records are found for the specified factory and operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.Wip"/>.</returns>
        [HttpGet("{factory}/{opcd}/async")]
        [ProducesResponseType(typeof(Wip), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync(string opcd, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(opcd))
            {
                return BadRequest();
            }

            Wip wip = await _wipRepo.GetWipAsync(opcd, factory);

			if (wip == null)
            {
                return NotFound();
            }

            return Ok(wip);
		}

        /// <summary>
        /// Wip Info by Factory,Operation File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the WIP information for the specified factory and operation code.</response>
        /// <response code="400">If the operation code is missing or empty or if the provided file format is not supported.</response>
        /// <response code="404">If no WIP records are found for the specified factory and operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.Wip"/>.</returns>
        [HttpGet("{factory}/{opcd}/file")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(Wip), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetFile(string opcd, string factory = "DS", string format = "json")
		{
			if (String.IsNullOrWhiteSpace(opcd))
            {
                return BadRequest();
            }

            Wip wip = _wipRepo.GetWip(opcd, factory);

			if (wip == null)
            {
                return NotFound();
            }

            string filename = $"{opcd.ToLower()}_wip";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _wipJson.Download(wip, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _wipCsv.Download(wip, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _wipXml.Download(wip, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        #endregion

        #region Controllers - WIP : WorkList

        /// <summary>
        /// All Wip By Production Info.
        /// </summary>
        /// <response code="200">Returns the list of active work items.</response>
        /// <response code="404">If no work items are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkList()
		{
			List<SampleWork> workList = _wipRepo.GetWorkList() as List<SampleWork> ?? null!;

			if (workList == null)
            {
                return NotFound();
            }

            return Ok(workList);
		}

        /// <summary>
        /// (async) All Wip By Production Info.
        /// </summary>
        /// <response code="200">Returns the list of active work items.</response>
        /// <response code="404">If no work items are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/async")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetWorkListAsync()
		{
			List<SampleWork> workList = await _wipRepo.GetWorkListAsync() as List<SampleWork> ?? null!;

			if (workList == null)
            {
                return NotFound();
            }

            return Ok(workList);
		}

        /// <summary>
        /// All Wip By Production Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the file containing active work items.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no work items are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/file")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(FileStreamResult), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkListFile(string format = "json")
		{
			List<SampleWork> workList = _wipRepo.GetWorkList() as List<SampleWork> ?? null!;

			if (workList == null)
            {
                return NotFound();
            }

            string filename = "wip_worklist";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _workJson.Download(workList, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _workCsv.Download(workList, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _workXml.Download(workList, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Wip By Production,Operation Info.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code</param>
        /// <response code="200">Returns the list of active work items for the specified factory and operation.</response>
        /// <response code="404">If no work items are found for the specified factory and operation.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/></returns>
        [HttpGet("worklist/{factory}/{opcd}")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkListBy(string opcd, string factory = "DS")
		{
			List<SampleWork> workList = _wipRepo.GetWorkList(opcd, factory) as List<SampleWork> ?? null!;

			if (workList == null)
            {
                return NotFound();
            }

            return Ok(workList);
		}

        /// <summary>
        /// (async) Wip By Production,Operation Info.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code</param>
        /// <response code="200">Returns the list of active work items for the specified factory and operation.</response>
        /// <response code="404">If no work items are found for the specified factory and operation.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/></returns>
        [HttpGet("worklist/{factory}/{opcd}/async")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetWorkListByAsync(string opcd, string factory = "DS")
		{
			List<SampleWork> workList = await _wipRepo.GetWorkListAsync(opcd, factory) as List<SampleWork> ?? null!;

			if (workList == null)
            {
                return NotFound();
            }

            return Ok(workList);
		}

        /// <summary>
        /// Wip By Production,Operation Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns the list of active work items for the specified factory and operation.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no work items are found for the specified factory and operation.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.SampleWork"/>.</returns>
        [HttpGet("worklist/{factory}/{opcd}/file")]
        [Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetWorkListByFile(string opcd, string factory = "DS", string format = "json")
		{
			List<SampleWork> workList = _wipRepo.GetWorkList(opcd, factory) as List<SampleWork> ?? null!;

			if (workList == null)
            {
                return NotFound();
            }

            string filename = $"wip_worklist_{factory.ToLower()}_{opcd.ToLower()}";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _workJson.Download(workList, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _workCsv.Download(workList, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _workXml.Download(workList, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Wip By Production,Operation Info.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="keyword">Search Keyword.</param>
        /// <response code="200">Returns a list of active work items matching the search criteria.</response>
        /// <response code="400">If both factory and operation code are missing or empty.</response>
        /// <response code="404">If no work items are found matching the search criteria.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/></returns>
        [HttpGet("worklist/{factory}/{opcd}/keyword")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSearch(string opcd, string? keyword, string factory = "DS")
		{
			if (string.IsNullOrEmpty(factory) & string.IsNullOrEmpty(opcd))
            {
                return BadRequest();
            }

            List<SampleWork> workList = _wipRepo.GetSearch(opcd, keyword, factory) as List<SampleWork> ?? null!;

			if (workList == null)
            {
                return NotFound();
            }

            return Ok(workList);
		}


        /// <summary>
        /// (async) Wip By Production,Operation Info.
        /// </summary>
        /// <param name="factory">Factory Code (default is DS).</param>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="keyword">Search Keyword.</param>
        /// <response code="200">Returns a list of active work items matching the search criteria.</response>
        /// <response code="400">If both factory and operation code are missing or empty.</response>
        /// <response code="404">If no work items are found matching the search criteria.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/></returns>
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

            List<SampleWork> workList = await _wipRepo.GetSearchAsync(opcd, keyword, factory) as List<SampleWork> ?? null!;


			if (workList == null)
            {
                return NotFound();
            }

            return Ok(workList);
		}

        #endregion

        #region Controllers - WIP : Rate

        /// <summary>
        /// Wip Rate Info.
        /// </summary>
        /// <response code="200">Returns a list of active WIP rates.</response>
        /// <response code="404">If no WIP rates are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.WipRate"/></returns>
        [HttpGet("rates")]
        [ProducesResponseType(typeof(List<WipRate>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRates()
		{
			List<WipRate> rates = _rateRepo.GetWipRates() as List<WipRate> ?? null!;
			
			if(rates == null)
            {
                return NotFound();
            }

            return Ok(rates);
		}

        /// <summary>
        /// (async) Wip Rate Info.
        /// </summary>
        /// <response code="200">Returns a list of active WIP rates.</response>
        /// <response code="404">If no WIP rates are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.WipRate"/></returns>
        [HttpGet("rates/async")]
        [ProducesResponseType(typeof(List<WipRate>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetRatesAsync()
        {
            List<WipRate> rates = await _rateRepo.GetWipRatesAsync() as List<WipRate> ?? null!;

            if (rates == null)
            {
                return NotFound();
            }

            return Ok(rates);
        }

        /// <summary>
        /// Wip Rate By Opcd Info.
        /// </summary>
        /// <param name="opcd">Operation Code.</param>
        /// <response code="200">Returns the WIP rate for the specified operation code.</response>
        /// <response code="404">If no WIP rate is found for the specified operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.WipRate"/></returns>
        [HttpGet("rates/{opcd}")]
        [ProducesResponseType(typeof(WipRate), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRate(string opcd)
        {
			WipRate rate = _rateRepo.GetWipRate(opcd);

            if (rate == null)
            {
                return NotFound();
            }

            return Ok(rate);
        }

        /// <summary>
        /// (async) Wip Rate By Opcd Info.
        /// </summary>
        /// <param name="opcd">Operation Code.</param>
        /// <response code="200">Returns the WIP rate for the specified operation code.</response>
        /// <response code="404">If no WIP rate is found for the specified operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.WipRate"/></returns>
        [HttpGet("rates/{opcd}/async")]
        [ProducesResponseType(typeof(WipRate), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetRateAsync(string opcd)
        {
			WipRate rate = await _rateRepo.GetWipRateAsync(opcd);

            if (rate == null)
            {
                return NotFound();
            }

            return Ok(rate);
        }

        #endregion

        #region Controllers - WIP : Rate Work List

        /// <summary>
        /// Work List in Wip Rate By Opcd, Status Info
        /// </summary>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="status">Status (I: 입고, T: 투입, O: 완료, K: 보관, S: 중단, H: 대기, A: 도착, P: 발행, D: 삭제).</param>
        /// <response code="200">Returns the list of active work items.</response>
        /// <response code="404">If no work items are found for the specified operation code and status.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/></returns>
        [HttpGet("rates/{opcd}/{status}")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetRateWorkList(string opcd, string status)
        {
            ICollection<SampleWork> worklist = _rateRepo.GetSampleKeys(opcd, status) as List<SampleWork> ?? null!;

            if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
        }

        /// <summary>
        /// (async) Work List in Wip Rate By Opcd, Status Info
        /// </summary>
        /// <param name="opcd">Operation Code.</param>
        /// <param name="status">Status (I: 입고, T: 투입, O: 완료, K: 보관, S: 중단, H: 대기, A: 도착, P: 발행, D: 삭제).</param>
        /// <response code="200">Returns the list of active work items.</response>
        /// <response code="404">If no work items are found for the specified operation code and status.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.SampleWork"/></returns>
        [HttpGet("rates/{opcd}/{status}/async")]
        [ProducesResponseType(typeof(List<SampleWork>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetRateWorkListAsync(string opcd, string status)
        {
            ICollection<SampleWork> worklist = await _rateRepo.GetSampleKeysAsync(opcd, status) as List<SampleWork> ?? null!;

            if (worklist == null)
            {
                return NotFound();
            }

            return Ok(worklist);
        }

        #endregion
    }
}
