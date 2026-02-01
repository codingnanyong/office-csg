using CSG.MI.DTO.DS;
using CSG.MI.FDW.BLL.HQ.RTLS.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
	/// <summary>
	/// RTLS) Current Sample API.
	/// </summary>
	[ApiController]
	[Route("v{version:apiVersion}/rtls/[controller]")]
	[ApiVersion("1.5")]
	public class RtlsSamplesController : Controller
	{
		#region Constructors

		private readonly IRtlsSampleRepo _repo;

		private readonly ICsvResultService<Sample> _csv;

		private readonly IJsonResultService<Sample> _json;

		private readonly IXmlResultService<Sample> _xml;

		private readonly ICsvResultService<SampleDetail> _csv2;

		private readonly IJsonResultService<SampleDetail> _json2;

		private readonly IXmlResultService<SampleDetail> _xml2;

		private readonly ILoggerManager _logger;

		/// <summary>
		/// SampleController Constructor
		/// </summary>
		/// <param name="repo"></param>
		/// <param name="logger"></param>
		/// <param name="csv"></param>
		/// <param name="json"></param>
		/// <param name="xml"></param>
		/// <param name="csv2"></param>
		/// <param name="json2"></param>
		/// <param name="xml2"></param>
		public RtlsSamplesController(IRtlsSampleRepo repo, ILoggerManager logger,
								ICsvResultService<Sample> csv, IJsonResultService<Sample> json, IXmlResultService<Sample> xml,
								ICsvResultService<SampleDetail> csv2, IJsonResultService<SampleDetail> json2, IXmlResultService<SampleDetail> xml2)
		{
			_repo = repo;
			_csv = csv;
			_json = json;
			_xml = xml;
			_csv2 = csv2;
			_json2 = json2;
			_xml2 = xml2;
			_logger = logger;
		}

		#endregion

		#region Sample

		/// <summary>
		/// Current Process Samples Info(RTLS).
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetAll()
		{
			ICollection<Sample> plans = _repo.GetLocation();

			if (plans == null)
				return NotFound();

			return Ok(plans);
		}

		/// <summary>
		/// Current Process Samples Info(RTLS) async call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetAllAsync()
		{
			ICollection<Sample> plans = await _repo.GetLocationAsync();

			if (plans == null)
				return NotFound();

			return Ok(plans);
		}

		/// <summary>
		/// Current Process Samples Info(RTLS) File(JSON,CSV,XML) Download.
		/// </summary>
		/// <param name="format">Download File Format</param>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("file")]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetAllFile(string format = "json")
		{
			ICollection<Sample> rst = _repo.GetLocation();

			if (rst == null)
				return NotFound();

			if (format.ToLower() == "json")
			{
				var stream = _json.Download(rst, "result_list.json");
				return File(stream, "application/json", "result_list.json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv.Download(rst, "result_list.csv");
				return File(stream, "text/csv", "result_list.csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml.Download((List<Sample>)rst, "result_list.xml");
				return File(stream, "application/xml", "result_list.xml");
			}
			else
				return BadRequest();
		}

		/// <summary>
		/// Selected Process(Factory) Samples Info(RTLS).
		/// </summary>
		/// <param name="opcd">Operation Code.</param>
		/// <param name="factory">Factory Code.</param>
		/// <response code="400">Bad Request</response>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("{factory}/{opcd}")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult Get(string opcd, string factory = "DS")
		{
			if (String.IsNullOrEmpty(opcd))
				return BadRequest();

			ICollection<Sample> rst = _repo.GetLocation(opcd);

			if (rst == null)
				return NotFound();

			return Ok(rst);
		}

		/// <summary>
		/// Selected Process(Factory) Samples Info(RTLS) Async Call.
		/// </summary>
		/// <param name="opcd">Operation Code.</param>
		/// <param name="factory">Factory Code</param>
		/// <response code="400">Bad Request</response>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("{factory}/{opcd}/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetAsync(string opcd, string factory = "DS")
		{
			if (String.IsNullOrEmpty(opcd))
				return BadRequest();

			ICollection<Sample> rst = await _repo.GetLocationAsync(opcd, factory);

			if (rst == null)
				return NotFound();

			return Ok(rst);
		}

		/// <summary>
		/// Selected Process(Factory) Samples Info(RTLS) File(JSON,CSV,XML) Download..
		/// </summary>
		/// <param name="opcd">Operation Code.</param>
		/// <param name="factory">Factory Code</param>
		/// <param name="format">Download File Format</param>
		/// <response code="400">Bad Request</response>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("{factory}/{opcd}/file")]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetFile(string opcd, string factory = "DS", string format = "json")
		{
			if (String.IsNullOrEmpty(opcd))
				return BadRequest();

			ICollection<Sample> rst = _repo.GetLocation(opcd, factory);

			if (rst == null)
				return NotFound();


			if (format.ToLower() == "json")
			{
				var stream = _json.Download(rst, "get_list.json");
				return File(stream, "application/json", "get_list.json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv.Download(rst, "get_list.csv");
				return File(stream, "text/csv", "get_list.csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml.Download((List<Sample>)rst, "get_list.xml");
				return File(stream, "application/xml", "get_list.xml");
			}
			else
				return BadRequest();
		}

		#endregion

		#region Sample Detail

		/// <summary>
		/// Selected Sample(BOM,WorkSheet) Info(RTLS History).
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <param name="wsno">WorkSheet Code</param>
		/// <returns><see cref="CSG.MI.DTO.DS.SampleDetail"/></returns>
		[HttpGet("{factory}/detail")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetDetil([Required] string wsno, string factory = "DS")
		{
			SampleDetail sampledetail = _repo.Get(wsno, factory);

			if (sampledetail == null)
				return NotFound();

			return Ok(sampledetail);
		}

		/// <summary>
		/// Selected Sample(BOM,WorkSheet) Info(RTLS History) Async Call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <param name="wsno">WorkSheet Code</param>
		/// <returns><see cref="CSG.MI.DTO.DS.SampleDetail"/></returns>
		[HttpGet("{factory}/detail/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetDetilAsync([Required] string wsno, string factory = "DS")
		{
			SampleDetail sampledetail = await _repo.GetAsync(wsno, factory);

			if (sampledetail == null)
				return NotFound();

			return Ok(sampledetail);
		}

		/// <summary>
		/// Seected Sample(BOM,WorkSheet) Info(RTLS History) File(JSON,CSV,XML) Download.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="format">File Format</param>
		/// <param name="factory">Factory Code</param>
		/// <param name="wsno">WorkSheet Code</param>
		/// <returns><see cref="CSG.MI.DTO.DS.SampleDetail"/></returns>
		[HttpGet("{factory}/detail/file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetDetilFile([Required] string wsno, string factory = "DS", string format = "json")
		{
			SampleDetail sampledetail = _repo.Get(wsno, factory);

			if (sampledetail == null)
				return NotFound();

			if (format.ToLower() == "json")
			{
				var stream = _json2.Download(sampledetail, "SampleDetail.json");
				return File(stream, "application/json", "SampleDetail.json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv2.Download(sampledetail, "SampleDetail.csv");
				return File(stream, "text/csv", "SampleDetail.csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml2.Download(sampledetail, "SampleDetail.xml");
				return File(stream, "application/xml", "SampleDetail.xml");
			}
			else
				return BadRequest();
		}

		#endregion
	}
}