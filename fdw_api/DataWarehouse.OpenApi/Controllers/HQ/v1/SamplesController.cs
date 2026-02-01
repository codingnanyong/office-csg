using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Production] Current Sample API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class SamplesController : ControllerBase
	{
		#region Constructors

		private readonly IWsSampleRepo _repo;
        private readonly ILoggerManager _logger;

        private readonly ICsvResultService<Sample> _csv;
		private readonly IJsonResultService<Sample> _json;
		private readonly IXmlResultService<Sample> _xml;

        /// <summary>
        /// SampleController Constructor
        /// </summary>
        /// <param name="repo">[Production] Samples Repository</param>
        /// <param name="logger">Logging Manager</param>
        /// <param name="csv">[Production] Samples data to CSV</param>
        /// <param name="json">[Production] Samples data to JSON</param>
        /// <param name="xml">[Production] Samples data to XML</param>
        public SamplesController(IWsSampleRepo repo, ILoggerManager logger,ICsvResultService<Sample> csv, IJsonResultService<Sample> json, IXmlResultService<Sample> xml)
		{
			_repo = repo;
			_csv = csv;
			_json = json;
			_xml = xml;
			_logger = logger;
		}

        #endregion

        #region Sample

        /// <summary>
        /// [Not recommended] Current Process Samples Info.
        /// </summary>
        /// <response code="200">Returns a list of active samples.</response>
        /// <response code="404">If no samples are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/></returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<Sample>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSamples()
		{
			List<Sample> samples = _repo.GetSamples() as List<Sample> ?? null!;

			if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);
		}

        /// <summary>
        /// [Not recommended] (async) Current Process Samples Info.
        /// </summary>
        /// <response code="200">Returns a list of active samples.</response>
        /// <response code="404">If no samples are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/></returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetSamplesAsync()
		{
			List<Sample> samples = await _repo.GetSamplesAsync() as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);

		}

        /// <summary>
        /// Current Process Samples Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns a file containing active samples.</response>
        /// <response code="400">If the provided file format is not supported.</response>
        /// <response code="404">If no samples are found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active <see cref="CSG.MI.DTO.Production.Sample"/></returns>
        [HttpGet("file")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSamplesFile(string format = "json")
		{
			List<Sample> samples = _repo.GetSamples() as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            string filename = "samples";

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(samples, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(samples, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(samples, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Selected Worksheet Sample Info.
        /// </summary>
        /// <param name="wsno">Worksheet number.</param>
        /// <param name="factory">Factory code (default is "DS").</param>
        /// <response code="200">Returns the selected sample information.</response>
        /// <response code="400">If the worksheet number is invalid.</response>
        /// <response code="404">If the sample is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>The selected <see cref="CSG.MI.DTO.Production.Sample"/>.</returns>
        [HttpGet("{factory}/{wsno}")]
        [ProducesResponseType(typeof(Sample),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSample(string wsno, string factory = "DS")
		{
            if (string.IsNullOrEmpty(wsno))
            {
                return BadRequest("Worksheet number is required.");
            }

            Sample sample = _repo.GetSample(wsno, factory);

			if (sample == null)
            {
                return NotFound();
            }

            return Ok(sample);
		}

        /// <summary>
        /// (async) Selected Worksheet Sample Info.
        /// </summary>
        /// <param name="wsno">Worksheet number.</param>
        /// <param name="factory">Factory code (default is "DS").</param>
        /// <response code="200">Returns the selected sample information.</response>
        /// <response code="400">If the worksheet number is invalid.</response>
        /// <response code="404">If the sample is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a <see cref="CSG.MI.DTO.Production.Sample"/></returns>
        [HttpGet("{factory}/{wsno}/async")]
        [ProducesResponseType(typeof(Sample), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetSampleAsync(string wsno, string factory = "DS")
		{
            if (string.IsNullOrEmpty(wsno))
            {
                return BadRequest("Worksheet number is required.");
            }

            Sample sample = await _repo.GetSampleAsync(wsno, factory);

			if (sample == null)
            {
                return NotFound();
            }

            return Ok(sample);
		}

        /// <summary>
        /// Selected Worksheet Sample Info File(JSON,CSV,XML) Download.
        /// </summary>
        /// <param name="wsno">Worksheet number.</param>
        /// <param name="factory">Factory code (default is "DS").</param>
        /// <param name="format">File format to download (json, csv, xml).</param>
        /// <response code="200">Returns a file containing the selected sample information.</response>
        /// <response code="400">If the worksheet number or if the provided file format is not supported.</response>
        /// <response code="404">If the sample is not found.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a file containing active e<see cref="CSG.MI.DTO.Production.Sample"/></returns>
        [HttpGet("{factory}/{wsno}/file")]
        [ProducesResponseType(typeof(Sample), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSampleFile(string wsno, string factory = "DS", string format = "json")
		{
			Sample sample = _repo.GetSample(wsno, factory);

			if (sample == null)
            {
                return NotFound();
            }

            string filename = "sample_" + wsno.ToLower();

            switch (format.ToLower())
            {
                case "json":
                    var jsonStream = _json.Download(sample, filename + ".json");
                    return File(jsonStream, "application/json", filename + ".json");

                case "csv":
                    var csvStream = _csv.Download(sample, filename + ".csv");
                    return File(csvStream, "text/csv", filename + ".csv");

                case "xml":
                    var xmlStream = _xml.Download(sample, filename + ".xml");
                    return File(xmlStream, "application/xml", filename + ".xml");

                default:
                    return BadRequest("Invalid file format. Supported formats are JSON, CSV, XML.");
            }
        }

        /// <summary>
        /// Search Sample Info by Keyword.
        /// </summary>
        /// <param name="keyword">Keyword to search for.</param>
        /// <response code="200">Returns a list of samples matching the keyword.</response>
        /// <response code="404">If no samples match the keyword.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/>.</returns>
        [HttpGet("search")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult SrhSamples(string keyword)
        {
            List<Sample> samples = _repo.SearchSamples(keyword) as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);
        }

        /// <summary>
        /// (async) Search Sample Info by Keyword.
        /// </summary>
        /// <param name="keyword">Keyword to search for.</param>
        /// <response code="200">Returns a list of samples matching the keyword.</response>
        /// <response code="404">If no samples match the keyword.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/>.</returns>
        [HttpGet("search/async")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> SrhSamplesAsync(string keyword)
        {
            List<Sample> samples = await _repo.SearchSamplesAsync(keyword) as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);
        }

        /// <summary>
        /// Samples located by opcd.
        /// </summary>
        /// <param name="opcd">Operation code.</param>
        /// <response code="200">Returns a list of samples located by the operation code.</response>
        /// <response code="404">If no samples are found for the operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/>.</returns>
        [HttpGet("process")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSamplesByOp([Required]string opcd)
        {
            List<Sample> samples = _repo.GetSamplesByOp(opcd) as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);
        }

        /// <summary>
        /// (async) Samples located by opcd.
        /// </summary>
        /// <param name="opcd">Operation code.</param>
        /// <response code="200">Returns a list of samples located by the operation code.</response>
        /// <response code="404">If no samples are found for the operation code.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/>.</returns>
        [HttpGet("process/async")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetSamplesByOpAsync([Required] string opcd)
        {
            List<Sample> samples = await _repo.GetSamplesByOpAsync(opcd) as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);
        }

        /// <summary>
        /// Samples located by opcd,status.
        /// </summary>
        /// <param name="opcd">Operation code.</param>
        /// <param name="status">Status (I: 입고, T: 투입, O: 완료, K: 보관, S: 중단, H: 대기, A: 도착, P: 발행, D: 삭제).</param>
        /// <response code="200">Returns a list of samples located by the operation code and status.</response>
        /// <response code="404">If no samples are found for the operation code and status.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/>.</returns>
        [HttpGet("process/status")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetSamplesByOpStatus([Required] string opcd, [Required] string status)
        {
            List<Sample> samples = _repo.GetSamplesByOpStatus(opcd,status) as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);
        }

        /// <summary>
        /// (async) Samples located by opcd,status.
        /// </summary>
        /// <param name="opcd">Operation code.</param>
        /// <param name="status">Status (I: 입고, T: 투입, O: 완료, K: 보관, S: 중단, H: 대기, A: 도착, P: 발행, D: 삭제).</param>
        /// <response code="200">Returns a list of samples located by the operation code and status.</response>
        /// <response code="404">If no samples are found for the operation code and status.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Production.Sample"/>.</returns>
        [HttpGet("process/status/async")]
        [ProducesResponseType(typeof(List<Sample>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetSamplesByOpStatusAsync([Required] string opcd, [Required] string status)
        {
            List<Sample> samples = await _repo.GetSamplesByOpStatusAsync(opcd,status) as List<Sample> ?? null!;

            if (samples == null)
            {
                return NotFound();
            }

            return Ok(samples);
        }

        #endregion
    }
}
