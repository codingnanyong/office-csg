using System;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Fsw
{
	/// <summary>
	/// 폴더경로 유효 이벤트 인자 클래스.
	/// </summary>
	public class PathAvailablitiyEventArgs : EventArgs
	{
		/// <summary>
		/// 기본 생성자
		/// </summary>
		/// <param name="available">경로 유효 여부</param>
		public PathAvailablitiyEventArgs(bool available)
		{
			this.PathIsAvailable = available;
		}

		/// <summary>
		/// 경로 유효 여부
		/// </summary>
		public bool PathIsAvailable { get; set; }
	}
}
