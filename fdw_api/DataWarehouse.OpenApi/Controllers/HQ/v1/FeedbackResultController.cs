using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.BLL.UserFeedback.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
	/// <summary>
	/// Feedback Result API.
	/// </summary>
	[ApiController]
	[Route("v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
	public class FeedbackResultController : ControllerBase
	{
		#region Constructors

		private readonly IFeedbackResultRepo _repo;
		private readonly ILoggerManager _logger;

		private readonly ICsvResultService<Feedback> _csv;
		private readonly IJsonResultService<Feedback> _json;
		private readonly IXmlResultService<Feedback> _xml;

		/// <summary>
		/// FeedbackController Constructor Definition
		/// </summary>
		/// <param name="repo"></param>
		/// <param name="csv"></param>
		/// <param name="json"></param>
		/// <param name="xml"></param>
		/// <param name="logger"></param>
		public FeedbackResultController(IFeedbackResultRepo repo, ILoggerManager logger,
										 ICsvResultService<Feedback> csv, IJsonResultService<Feedback> json, IXmlResultService<Feedback> xml)
		{
			_repo = repo;
			_logger = logger;
			_csv = csv;
			_json = json;
			_xml = xml;

		}

		#endregion

		#region FeedbackResult CRUD - Create

		/// <summary>
		/// Create New Feedback Result.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpPost]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status201Created, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult Create([FromBody] FeedbackResult result)
		{
			if (result == null)
				return BadRequest();

			if (string.IsNullOrWhiteSpace(result.FeedbackId))
				ModelState.AddModelError("FeedbackId", "Feedback Id shouldn't be empty");

			if (ModelState.IsValid == false)
				return UnprocessableEntity(ModelState);

			var item = _repo.CreateFeedbackResult(result);
			if (item == null)
				return Conflict();

			return Created("FeedbackResult", item);
		}

		/// <summary>
		/// Create New FeedbackResult Async.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpPost("async")]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status201Created, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> CreateAsync([FromBody] FeedbackResult result)
		{
			if (result == null)
				return BadRequest();

			if (string.IsNullOrWhiteSpace(result.FeedbackId))
				ModelState.AddModelError("FeedbackId", "Feedback Id shouldn't be empty");

			if (ModelState.IsValid == false)
				return UnprocessableEntity(ModelState);

			var item = await _repo.CreateFeedbackResultAsync(result);
			if (item == null)
				return Conflict();

			return Created("FeedbackResult", item);
		}

		#endregion

		#region FeedbackResult CRUD - Read

		/// <summary>
		/// All FeedbackResult Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpGet]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetAll()
		{
			ICollection<FeedbackResult> results = _repo.GetFeedbackResults();

			if (results == null)
				return NotFound();

			return Ok(results);

		}

		/// <summary>
		/// All FeedbackResult Info Async.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpGet("async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetAllAsync()
		{
			ICollection<FeedbackResult> results = await _repo.GetFeedbackResultsAsync();

			if (results == null)
				return NotFound();

			return Ok(results);

		}

		/// <summary>
		/// FeedbackResult Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpGet("{id}")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult Get(string id)
		{
			FeedbackResult result = _repo.GetFeedbackResult(id);

			if (result == null)
				return NotFound();

			return Ok(result);

		}

		/// <summary>
		/// FeedbackResult Info Async.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpGet("{id}/async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetAsync(string id)
		{
			FeedbackResult result = await _repo.GetFeedbackResultAsync(id);

			if (result == null)
				return NotFound();

			return Ok(result);

		}

		#endregion

		#region FeedbackResult CRUD - Update

		/// <summary>
		/// Update FeedbackResult Info.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpPut]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult Update([FromBody] FeedbackResult result)
		{
			if (result == null)
				return BadRequest();

			if (string.IsNullOrWhiteSpace(result.FeedbackId))
				ModelState.AddModelError("FeedbackId", "Feedback Id shouldn't be empty");

			if (ModelState.IsValid == false)
				return UnprocessableEntity(ModelState);

			var item = _repo.UpdateFeedbackResult(result);
			if (item == null)
				return Conflict();

			return NoContent();
		}

		/// <summary>
		/// Update FeedbackResult Info.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpPut("async")]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status422UnprocessableEntity, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> UpdateAsync([FromBody] FeedbackResult result)
		{
			if (result == null)
				return BadRequest();

			if (string.IsNullOrWhiteSpace(result.FeedbackId))
				ModelState.AddModelError("FeedbackId", "Feedback Id shouldn't be empty");

			if (ModelState.IsValid == false)
				return UnprocessableEntity(ModelState);

			var item = await _repo.UpdateFeedbackResultAsync(result);
			if (item == null)
				return Conflict();

			return NoContent();
		}

		#endregion

		#region Feedback CRUD - Delete

		/// <summary>
		/// Delete FeedbackResult Info.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpDelete("{id}")]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult Delete(string id)
		{
			if (string.IsNullOrWhiteSpace(id))
				return BadRequest();

			var affected = _repo.Delete(id);
			if (affected == 0)
				return Conflict();

			return NoContent();
		}

		/// <summary>
		/// Delete FeedbackResult Info.
		/// </summary>
		/// <returns>List of active<see cref="CSG.MI.DTO.Feedback.FeedbackResult"/></returns>
		[HttpDelete("{id}/async")]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
		[ProducesResponseType(StatusCodes.Status204NoContent, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(FeedbackResult))]
		[ProducesResponseType(StatusCodes.Status400BadRequest, Type = typeof(string))]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> DeleteAsync(string id)
		{
			if (string.IsNullOrWhiteSpace(id))
				return BadRequest();

			var affected = await _repo.DeleteAsync(id);
			if (affected == 0)
				return Conflict();

			return NoContent();
		}

		#endregion
	}
}
