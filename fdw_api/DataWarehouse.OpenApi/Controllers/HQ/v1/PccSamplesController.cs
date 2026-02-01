using CSG.MI.DTO.DS;
using CSG.MI.FDW.BLL.HQ.PCC.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
	/// <summary>
	/// PCC) Current Sample API.
	/// </summary>
	[ApiController]
	[Route("v{version:apiVersion}/pcc/[controller]")]
	[ApiVersion("1.5")]
	public class PccSamplesController : ControllerBase
	{
		#region Constructors

		private readonly IPccSampleRepo _repo;

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
		public PccSamplesController(IPccSampleRepo repo, ILoggerManager logger,
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
		/// Current Process Samples Info(PCC).
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetByProcess()
		{
			ICollection<Sample> samples = _repo.GetLocation();

			if (samples == null)
				return NotFound();

			return Ok(samples);
		}

		/// <summary>
		/// Current Process Samples Info(PCC) Async Call.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetByProcessAsync()
		{
			ICollection<Sample> samples = await _repo.GetLocationAsync();

			if (samples == null)
				return NotFound();

			return Ok(samples);

		}

		/// <summary>
		/// Current Process Samples Info(PCC) File(JSON,CSV,XML) Download.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetByProcessFile(string format = "json")
		{
			ICollection<Sample> samples = _repo.GetLocation();

			if (samples == null)
				return NotFound();

			if (format.ToLower() == "json")
			{
				var stream = _json.Download(samples, "samples.json");
				return File(stream, "application/json", "samples.json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv.Download(samples, "samples.csv");
				return File(stream, "text/csv", "samples.csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml.Download((List<Sample>)samples, "samples.xml");
				return File(stream, "application/xml", "samples.xml");
			}
			else
				return BadRequest();
		}

		/// <summary>
		/// Selected Process(Factory) Samples Info(PCC).
		/// </summary>
		/// <param name="opcd">Operation Code</param>
		/// <param name="factory">Factory Code</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("{factory}/{opcd}")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetByProcess(string opcd, string factory = "DS")
		{
			ICollection<Sample> samples = _repo.GetLocation(opcd, factory);

			if (samples == null)
				return NotFound();

			return Ok(samples);
		}

		/// <summary>
		/// Selected Process(Factory) Samples Info(PCC) Async Call.
		/// </summary>
		/// <param name="opcd">Operation Code</param>
		/// <param name="factory">Factory Code</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("{factory}/{opcd}/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetByProcessAsync(string opcd, string factory = "DS")
		{
			ICollection<Sample> samples = await _repo.GetLocationAsync(opcd, factory);

			if (samples == null)
				return NotFound();

			return Ok(samples);
		}

		/// <summary>
		/// Selected Process(Factory) Samples Info(PCC) File(JSON,CSV,XML) Download.
		/// </summary>
		/// <param name="opcd">Operation Code</param>
		/// <param name="factory">Factory Code</param>
		/// <param name="format">File Format</param>
		/// <returns>List of active<see cref="CSG.MI.DTO.DS.Sample"/></returns>
		[HttpGet("{factory}/{opcd}/file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetByProcessFile(string opcd, string factory = "DS", string format = "json")
		{
			ICollection<Sample> samples = _repo.GetLocation(opcd, factory);

			if (samples == null)
				return NotFound();

			string filename = "sample_" + opcd.ToLower();

			if (format.ToLower() == "json")
			{
				var stream = _json.Download(samples, filename + ".json");
				return File(stream, "application/json", filename + ".json");
			}
			else if (format.ToLower() == "csv")
			{
				var stream = _csv.Download(samples, filename + ".csv");
				return File(stream, "text/csv", filename + ".csv");
			}
			else if (format.ToLower() == "xml")
			{
				var stream = _xml.Download((List<Sample>)samples, filename + ".xml");
				return File(stream, "application/xml", filename + ".xml");
			}
			else
				return BadRequest();
		}

		#endregion

		#region SampleDetail

		/// <summary>
		/// Selected Sample(BOM,WorkSheet) Info(PCC History).
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
		/// Selected Sample(BOM,WorkSheet) Info(PCC History) Async Call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="factory">Factory Code</param>
		/// <param name="wsno">WorkSheet Code</param>
		/// <returns><see cref="CSG.MI.DTO.DS.SampleDetail"/></returns>
		[HttpGet("{factory}/detail/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetDetailAsync([Required] string wsno, string factory = "DS")
		{
			SampleDetail sampledetail = await _repo.GetAsync(wsno, factory);

			if (sampledetail == null)
				return NotFound();

			return Ok(sampledetail);
		}

		/// <summary>
		/// Seected Sample(BOM,WorkSheet) Info(PCC History) File(JSON,CSV,XML) Download.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <param name="format">File Format</param>
		/// <param name="factory">Factory Code</param>
		/// <param name="wsno">WorkSheet Code</param>
		/// <returns><see cref="CSG.MI.DTO.DS.SampleDetail"/></returns>
		[HttpGet("{factory}/detail/file")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetDetailFile([Required] string wsno, string factory = "DS", string format = "json")
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
