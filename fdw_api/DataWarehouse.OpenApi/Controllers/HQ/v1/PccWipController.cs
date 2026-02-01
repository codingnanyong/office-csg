using CSG.MI.DTO.DS;
using CSG.MI.FDW.BLL.HQ.PCC.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
	/// <summary>
	/// PCC) Work In Process API.
	/// </summary>
	[ApiController]
	[Route("v{version:apiVersion}/pcc/[controller]")]
	[ApiVersion("1.5")]
	public class PccWipController : ControllerBase
	{
		#region Constructors

		private readonly IPccWipRepo _repo;

		private readonly ICsvResultService<Wip> _csv;

		private readonly IJsonResultService<Wip> _json;

		private readonly IXmlResultService<Wip> _xml;

		private readonly ICsvResultService<WorkList> _csv1;

		private readonly IJsonResultService<WorkList> _json1;

		private readonly IXmlResultService<WorkList> _xml1;


		private readonly ILoggerManager _logger;

		/// <summary>
		/// WipController Constructor Definition
		/// </summary>
		/// <param name="repo"></param>
		/// <param name="csv"></param>
		/// <param name="json"></param>
		/// <param name="xml"></param>
		/// <param name="logger"></param>
		public PccWipController(IPccWipRepo repo, ILoggerManager logger
						   , ICsvResultService<Wip> csv, IJsonResultService<Wip> json, IXmlResultService<Wip> xml
						   , ICsvResultService<WorkList> csv1, IJsonResultService<WorkList> json1, IXmlResultService<WorkList> xml1
						)
		{
			_repo = repo;
			_csv = csv;
			_json = json;
			_xml = xml;
			_csv1 = csv1;
			_json1 = json1;
			_xml1 = xml1;
			_logger = logger;
		}

		#endregion

		#region Controllers - WIP

		/// <summary>
		/// All Wip Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetAll()
		{
			ICollection<Wip> wips = _repo.GetAllWip();

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}

		/// <summary>
		/// All Wip Info Async Call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetAllAsync()
		{
			ICollection<Wip> wips = await _repo.GetAllWipAsync();

			if (wips == null)
				return NotFound();

			return Ok(wips);

		}

		/// <summary>
		/// All Wip Info File(JSON,CSV,XML) Download.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="format">File Format</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetAllFile(string format = "json")
		{
			ICollection<Wip> wips = _repo.GetAllWip();

			if (wips == null)
				return NotFound();

			if (format.ToLower() == "json")
			{
				var stream = _json.Download(wips, "wip_list.json");
				return File(stream, "application/json", "wip_list.json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv.Download(wips, "wip_list.csv");
				return File(stream, "text/csv", "wip_list.csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml.Download((List<Wip>)wips, "wip_list.xml");
				return File(stream, "application/xml", "wip_list.xml");
			}
			else
				return BadRequest();
		}

		/// <summary>
		/// Wip By Factory Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("{factory}")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetByFactory(string factory = "DS")
		{
			ICollection<Wip> wips = _repo.GetAllWip(factory);

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}

		/// <summary>
		/// Wip By Factory Info Async Call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("{factory}/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetByFactoryAsync(string factory = "DS")
		{
			ICollection<Wip> wips = await _repo.GetAllWipAsync();

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}

		/// <summary>
		/// Wip By Factory Info File(JSON,CSV,XML) Download.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <param name="format">File Format</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("{factory}/file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetByFactoryFile(string factory = "DS", string format = "json")
		{
			ICollection<Wip> wips = _repo.GetAllWip(factory);

			if (wips == null)
				return NotFound();

			if (format.ToLower() == "json")
			{
				var stream = _json.Download(wips, "wip_list.json");
				return File(stream, "application/json", "wip_list.json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv.Download(wips, "wip_list.csv");
				return File(stream, "text/csv", "wip_list.csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml.Download((List<Wip>)wips, "wip_list.xml");
				return File(stream, "application/xml", "wip_list.xml"); ;
			}
			else
				return BadRequest();
		}

		/// <summary>
		/// Wip Info by Factory,Operation.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("{factory}/{opcd}")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult Get(string opcd, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(opcd))
				return BadRequest();

			Wip wip = _repo.GetWip(opcd, factory);

			if (wip == null)
				return NotFound();

			return Ok(wip);
		}

		/// <summary>
		/// Wip Info by Factory,Operation Async Call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("{factory}/{opcd}/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetAsync(string opcd, string factory = "DS")
		{
			if (String.IsNullOrWhiteSpace(opcd))
				return BadRequest();

			Wip wip = await _repo.GetWipAsync(opcd, factory);

			if (wip == null)
				return NotFound();

			return Ok(wip);
		}

		/// <summary>
		/// Wip Info by Factory,Operation File(JSON,CSV,XML) Download.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <param name="opcd">Operation Code</param>
		/// <param name="format">File Format</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Wip"/></returns>
		[HttpGet("{factory}/{opcd}/file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetFile(string opcd, string factory = "DS", string format = "json")
		{
			if (String.IsNullOrWhiteSpace(opcd))
				return BadRequest();

			Wip wip = _repo.GetWip(opcd, factory);

			if (wip == null)
				return NotFound();

			string filename = opcd.ToLower() + "wip";

			if (format.ToLower() == "json")
			{
				var stream = _json.Download(wip, filename + ".json");
				return File(stream, "application/json", filename + ".json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv.Download(wip, filename + ".csv");
				return File(stream, "text/csv", filename + ".csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml.Download(wip, filename + ".xml");
				return File(stream, "application/xml", filename + ".xml"); ;
			}
			else
				return BadRequest();
		}

		#endregion

		#region Controllers - WIP/Prod WorkList

		/// <summary>
		/// All Wip By Production Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetProductions()
		{
			ICollection<WorkList> wips = _repo.WipByProductions();

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}

		/// <summary>
		/// All Wip By Production Info Async call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetProductionsAsync()
		{
			ICollection<WorkList> wips = await _repo.WipByProductionsAsync();

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}

		/// <summary>
		/// All Wip By Production Info File(JSON,CSV,XML) Download.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="format">File Format</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions/file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetProductionsFile(string format = "json")
		{
			ICollection<WorkList> wips = _repo.WipByProductions();

			if (wips == null)
				return NotFound();

			string filename = "wip_productions";

			if (format.ToLower() == "json")
			{
				var stream = _json1.Download(wips, filename + ".json");
				return File(stream, "application/json", filename + ".json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv1.Download(wips, filename + ".csv");
				return File(stream, "text/csv", filename + ".csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml1.Download((List<WorkList>)wips, filename + ".xml");
				return File(stream, "application/xml", filename + ".xml");
			}
			else
				return BadRequest();
		}

		/// <summary>
		/// Wip By Production,Operation Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <param name="opcd">Operation Code</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions/{factory}/{opcd}")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetProductions(string opcd, string factory = "DS")
		{
			ICollection<WorkList> wips = _repo.WipByProduction(opcd, factory);

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}


		/// <summary>
		/// Wip By Production,Operation Info Async call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions/{factory}/{opcd}/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetProductionsAsync(string opcd, string factory = "DS")
		{
			ICollection<WorkList> wips = await _repo.WipByProductionAsync(opcd, factory);

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}

		/// <summary>
		/// Wip By Production,Operation Info File(JSON,CSV,XML) Download.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions/{factory}/{opcd}/file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetProductionsFile(string opcd, string factory = "DS", string format = "json")
		{
			ICollection<WorkList> wips = _repo.WipByProduction(opcd, factory);

			if (wips == null)
				return NotFound();

			string filename = "wip_productions_" + factory.ToLower() + "_" + opcd.ToLower();

			if (format.ToLower() == "json")
			{
				var stream = _json1.Download(wips, filename + ".json");
				return File(stream, "application/json", filename + ".json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv1.Download(wips, filename + ".csv");
				return File(stream, "text/csv", filename + ".csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml1.Download((List<WorkList>)wips, filename + ".xml");
				return File(stream, "application/xml", filename + ".xml"); ;
			}
			else
				return BadRequest();
		}


		/// <summary>
		/// Wip By Production,Operation Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <param name="opcd">Operation Code</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions/{factory}/{opcd}/keyword")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetKeySearch(string opcd, string? keyword, string factory = "DS")
		{
			if (string.IsNullOrEmpty(factory) & string.IsNullOrEmpty(opcd))
				return BadRequest();

			ICollection<WorkList> wips = _repo.GetKeySearch(opcd, keyword, factory);

			if (wips == null)
				return NotFound();

			return Ok(wips);
		}


		/// <summary>
		/// Wip By Production,Operation Info Async call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.WorkList"/></returns>
		[HttpGet("productions/{factory}/{opcd}/keyword/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetKeySearchAsync(string opcd, string? keyword, string factory = "DS")
		{
			if (string.IsNullOrEmpty(factory) & string.IsNullOrEmpty(opcd))
				return BadRequest();

			ICollection<WorkList> wips = await _repo.GetKeySearchAsync(opcd, keyword, factory);


			if (wips == null)
				return NotFound();

			return Ok(wips);
		}

		#endregion
	}
}
