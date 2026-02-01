using CSG.MI.FDW.BLL.HQ.Interface;
using CSG.MI.FDW.Model.HQ.RTLS;
using Microsoft.AspNetCore.Mvc;

namespace CSG.MI.FDW.OpenApi.Controllers.HQ.v2
{
	/// <summary>
	/// Process Master Info.
	/// </summary>
	[ApiController]
	[Route("v{version:apiVersion}/rtls/[controller]")]
	[ApiVersion("2.0")]
	public class ProcessInfoController : ControllerBase
	{
		private readonly IProcessInfoRepo _repo;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="repo"></param>
		public ProcessInfoController(IProcessInfoRepo repo)
		{
			_repo = repo;
		}

		/// <summary>
		/// All Processes Info.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.FDW.Model.HQ.RTLS.ProcessInfo"/></returns>
		[HttpGet]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public IActionResult GetAll()
		{
			ICollection<ProcessInfo> processInfos = _repo.GetAll();
			if (processInfos == null)
				return NotFound();
			return Ok(processInfos);
		}

		/// <summary>
		/// All Processes Info async Call.
		/// </summary>
		/// <response code="404">Not Found</response>
		/// <returns>List of active<see cref="CSG.MI.FDW.Model.HQ.RTLS.ProcessInfo"/></returns>
		[HttpGet]
		[Route("async")]
		[ProducesResponseType(StatusCodes.Status200OK)]
		[ProducesResponseType(StatusCodes.Status500InternalServerError)]
		public async Task<IActionResult> GetAllAsync()
		{
			ICollection<ProcessInfo> processInfos = await _repo.GetAllAsync();
			if (processInfos == null)
				return NotFound();
			return Ok(processInfos);
		}
	}
}
