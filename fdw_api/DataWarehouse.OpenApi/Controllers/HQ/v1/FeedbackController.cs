using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.BLL.Feedbacks.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using System.ComponentModel.DataAnnotations;
using System.Net.Mime;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v1
{
    /// <summary>
    /// [Feedback] Feedback API.
    /// </summary>
    [ApiController]
	[Route("api/v{version:apiVersion}/[controller]")]
	[ApiVersion("1.5")]
    [ApiVersion("1.6")]
    public class FeedbackController : ControllerBase
	{
		#region Constructors

		private readonly IFeedbackRepo _repo;
		private readonly ILoggerManager _logger;

        /// <summary>
        /// FeedbackController Constructor Definition
        /// </summary>
        /// <param name="repo">Feedback Repository</param>
        /// <param name="logger">Logging Manager</param>
        public FeedbackController(IFeedbackRepo repo, ILoggerManager logger)
		{
			_repo = repo;
			_logger = logger;
		}

        #endregion

        #region Feedback - Create

        /// <summary>
        /// Create a New Feedback.
        /// </summary>
        /// <param name="feedback">The feedback object to create.</param>
        /// <response code="201">Created; indicates successful creation of the feedback.</response>
        /// <response code="400">Bad request; if the feedback object is null or the Category is empty.</response>
        /// <response code="409">Conflict; if there is a conflict while creating the feedback.</response>
        /// <response code="422">Unprocessable entity; if the feedback object is invalid.</response>
        /// <response code="500">Internal server error; if there is an unexpected server error.</response>
        /// <returns>The newly created <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpPost]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(Feedback))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(Feedback))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Create([FromBody] Feedback feedback)
		{
			if (feedback == null)
			{
                return BadRequest();
            }

			if (string.IsNullOrWhiteSpace(feedback.Category))
			{
                ModelState.AddModelError("Feedback_Category", "Category shouldn't be empty");
                return BadRequest(ModelState);
            }

			if (ModelState.IsValid == false)
			{
                return UnprocessableEntity(ModelState);
            }

			var newFeedback = _repo.CreateFeedback(feedback);

			if (newFeedback == null)
			{
                return UnprocessableEntity(ModelState);
            }

			return Created("Feedback", newFeedback);
		}

        /// <summary>
        /// (async) Create a New Feedback.
        /// </summary>
        /// <param name="feedback">The feedback object to create.</param>
        /// <response code="201">Created; indicates successful creation of the feedback.</response>
        /// <response code="400">Bad request; if the feedback object is null or the Category is empty.</response>
        /// <response code="409">Conflict; if there is a conflict while creating the feedback.</response>
        /// <response code="422">Unprocessable entity; if the feedback object is invalid.</response>
        /// <response code="500">Internal server error; if there is an unexpected server error.</response>
        /// <returns>The newly created <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpPost("async")]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status201Created, Type = typeof(Feedback))]
        [ProducesResponseType(StatusCodes.Status409Conflict, Type = typeof(Feedback))]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> CreateAsync([FromBody] Feedback feedback)
		{
			if (feedback == null)
			{
                return BadRequest();
            }

            if (string.IsNullOrWhiteSpace(feedback.Category))
            {
                ModelState.AddModelError("Feedback_Category", "Category shouldn't be empty");
                return BadRequest(ModelState);
            }

            if (ModelState.IsValid == false)
			{
                return UnprocessableEntity(ModelState);
            }

            var newFeedback = await _repo.CreateFeedbackAsync(feedback);

			if (newFeedback == null)
			{
                return Conflict();
            }

			return Created("Feedback", newFeedback);
		}

        #endregion

        #region Feedback - Read

        /// <summary>
        /// All Feedback Info.
        /// </summary>
        /// <param name="lang">Language code (e.g., "en", "kr").</param>
        /// <response code="200">Returns a list of active feedback entries.</response>
        /// <response code="400">If the language code is missing.</response>
        /// <response code="404">If no feedback entries are found for the specified language.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpGet]
        [ProducesResponseType(typeof(List<Feedback>),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult GetAll([Required] string lang)
		{
			if (lang == null)
            {
                return BadRequest();
            }

			ICollection<Feedback> feedbackList = _repo.GetFeedbacks(lang) ?? null!;

			if (feedbackList == null)
			{
                return NotFound();
            }

			return Ok(feedbackList);
		}

        /// <summary>
        /// (async) All Feedback Info.
        /// </summary>
        /// <param name="lang">Language code (e.g., "en", "kr").</param>
        /// <response code="200">Returns a list of active feedback entries.</response>
        /// <response code="400">If the language code is missing.</response>
        /// <response code="404">If no feedback entries are found for the specified language.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Returns a list of active <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpGet("async")]
        [ProducesResponseType(typeof(List<Feedback>), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAllAsync([Required] string lang)
		{
            if (lang == null)
            {
                return BadRequest();
            }

            ICollection<Feedback> feedbackList = await _repo.GetFeedbacksAsync(lang) ?? null!;

            if (feedbackList == null)
            {
                return NotFound();
            }

            return Ok(feedbackList);

		}

        /// <summary>
        /// Feedback Info.
        /// </summary>
        /// <param name="seq">Sequence.</param>
        /// <param name="sys">System Code.</param>
        /// <param name="lang">Language code (e.g., "en", "kr").</param>
        /// <response code="200">Returns the feedback information.</response>
        /// <response code="400">If the language code is missing.</response>
        /// <response code="404">If no feedback information is found for the specified parameters.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Return a <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpGet("{seq}/{sys}")]
        [ProducesResponseType(typeof(Feedback),StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Get(int seq, int sys,[Required] string lang)
		{
            if (lang == null)
            {
                return BadRequest();
            }

            Feedback feedback = _repo.GetFeedback(seq, sys, lang);

			if (feedback == null)
            {
                return NotFound();
            }

            return Ok(feedback);
		}

        /// <summary>
        /// (async) Feedback Info.
        /// </summary>
        /// <param name="seq">Sequence.</param>
        /// <param name="sys">System Code.</param>
        /// <param name="lang">Language code (e.g., "en", "kr").</param>
        /// <response code="200">Returns the feedback information.</response>
        /// <response code="400">If the language code is missing.</response>
        /// <response code="404">If no feedback information is found for the specified parameters.</response>
        /// <response code="500">If there is an internal server error.</response>
        /// <returns>Return a <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpGet("{seq}/{sys}/async")]
        [ProducesResponseType(typeof(Feedback), StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> GetAsync(int seq, int sys, [Required] string lang)
        {
            if (lang == null)
            {
                return BadRequest();
            }

            Feedback feedback = await _repo.GetFeedbackAsync(seq, sys, lang);

            if (feedback == null)
            {
                return NotFound();
            }

            return Ok(feedback);
		}

        #endregion

        #region Feedback - Update

        /// <summary>
        /// Update Feedback Info.
        /// </summary>
        /// <param name="feedback">The feedback object to update.</param>
        /// <response code="204">No content; indicates successful update.</response>
        /// <response code="400">Bad request; if the feedback object is null.</response>
        /// <response code="409">Conflict; if there is a conflict while updating.</response>
        /// <response code="422">Unprocessable entity; if the feedback object is invalid.</response>
        /// <response code="500">Internal server error; if there is an unexpected server error.</response>
        /// <returns>Returns <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpPut]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Update([FromBody] Feedback feedback)
		{
			if (feedback == null)
            {
                return BadRequest();
            }

            if (string.IsNullOrWhiteSpace(feedback.System) && feedback.Seq < 0)
            {
                ModelState.AddModelError("Feedback_Seq", "Feedback Sequnence and System shouldn't be empty");
                return BadRequest(ModelState);
            }

            if (ModelState.IsValid == false)
			{
                return UnprocessableEntity(ModelState);
            }
			
			var updateFeedback = _repo.UpdateFeedback(feedback);

			if (updateFeedback == null)
			{
                return Conflict();
            }

			return NoContent();
		}

        /// <summary>
        /// (async) Update Feedback Info.
        /// </summary>
        /// <param name="feedback">The feedback object to update.</param>
        /// <response code="204">No content; indicates successful update.</response>
        /// <response code="400">Bad request; if the feedback object is null.</response>
        /// <response code="409">Conflict; if there is a conflict while updating.</response>
        /// <response code="422">Unprocessable entity; if the feedback object is invalid.</response>
        /// <response code="500">Internal server error; if there is an unexpected server error.</response>
        /// <returns>Returns <see cref="CSG.MI.DTO.Feedback.Feedback"/>.</returns>
        [HttpPut("async")]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        [ProducesResponseType(StatusCodes.Status422UnprocessableEntity)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> UpdateAsync([FromBody] Feedback feedback)
		{
			if (feedback == null)
            {
                return BadRequest();
            }

            if (string.IsNullOrWhiteSpace(feedback.System) && feedback.Seq < 0)
            {
                ModelState.AddModelError("Feedback_Seq", "Feedback Sequnence and System shouldn't be empty");
				return BadRequest(ModelState);
            }

            if (ModelState.IsValid == false)
				return UnprocessableEntity(ModelState);

			var updateFeedback = await _repo.UpdateFeedbackAsync(feedback);

			if (updateFeedback == null)
            {
                return Conflict();
            }

            return NoContent();
		}

        #endregion

        #region Feedback - Delete

        /// <summary>
        /// Delete Feedback Info.
        /// </summary>
        /// <param name="seq">Sequence.</param>
        /// <param name="sys">System Code.</param>
        /// <response code="204">No content; indicates successful deletion of the feedback.</response>
        /// <response code="400">Bad request; if the sequence or system code is invalid (less than 0).</response>
        /// <response code="409">Conflict; if there is a conflict while deleting the feedback.</response>
        /// <response code="500">Internal server error; if there is an unexpected server error.</response>
        /// <returns>0 if the operation failed, otherwise returns the HTTP status code indicating success.</returns>
        [HttpDelete]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public IActionResult Delete([Required]int seq, [Required] int sys)
		{
            if (seq < 0 || sys < 0)
            {
                return BadRequest();
            }

            var deleteFeedback = _repo.Delete(seq, sys);

			if (deleteFeedback == 0)
            {
                return Conflict();
            }

            return NoContent();
		}

        /// <summary>
        /// (async) Delete Feedback Info.
        /// </summary>
        /// <param name="seq">Sequence.</param>
        /// <param name="sys">System Code.</param>
        /// <response code="204">No content; indicates successful deletion of the feedback.</response>
        /// <response code="400">Bad request; if the sequence or system code is invalid (less than 0).</response>
        /// <response code="409">Conflict; if there is a conflict while deleting the feedback.</response>
        /// <response code="500">Internal server error; if there is an unexpected server error.</response>
        /// <returns>0 if the operation failed, otherwise returns the HTTP status code indicating success.</returns>
        [HttpDelete("async")]
		[Consumes(MediaTypeNames.Application.Json)]
		[Produces(MediaTypeNames.Application.Json)]
        [ProducesResponseType(StatusCodes.Status204NoContent)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        [ProducesResponseType(StatusCodes.Status500InternalServerError)]
        public async Task<IActionResult> DeleteAsync([Required] int seq, [Required] int sys)
		{
            if (seq < 0 || sys < 0)
            {
                return BadRequest();
            }

            var deleteFeedback = await _repo.DeleteAsync(seq, sys);

			if (deleteFeedback == 0)
            {
                return Conflict();
            }

            return NoContent();
		}

		#endregion
	}
}
