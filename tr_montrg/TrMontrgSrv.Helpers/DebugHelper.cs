using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;

namespace CSG.MI.TrMontrgSrv.Helpers
{
	/// <summary>
	/// 디버깅 관련 도움 클래스
	/// </summary>
	public static class DebugHelper
	{
		#region Thread Safe Stopwatch

		/// <summary>
		/// 현시점에 대한 Stopwatch Tick을 반환한다.
		/// </summary>
		/// <returns>Tick 값</returns>
		public static long GetStopwatchTick()
		{
			return Stopwatch.GetTimestamp();
		}

		/// <summary>
		/// 시작시점과 현재시점에 소요된 시간을 ms 단위로 Output 화면에 표시한다.
		/// </summary>
		/// <param name="label">화면에 표시할 레이블(라벨)</param>
		/// <param name="startTick">미리 정의된 시작시점 Tick 값</param>
		/// <param name="logger">로거를 지정하면, 로그파일에도 소요시간을 남긴다</param>
		public static void WriteLineElapsedTime(string label, long startTick, Logger logger = null)
		{
			var stopTick = Stopwatch.GetTimestamp();
			WriteLineElapsedTime(label, startTick, stopTick, logger);
		}

		/// <summary>
		/// 시작시점과 종료시점에 소요된 시간을 ms 단위로 Output 화면에 표시한다.
		/// </summary>
		/// <param name="label">화면에 표시할 레이블(라벨)</param>
		/// <param name="startTick">미리 정의된 시작시점 Tick 값</param>
		/// <param name="stopTick">미리 정의된 종료시점 Tick 값</param>
		/// <param name="logger">로거를 지정하면, 로그파일에도 소요시간을 남긴다</param>
		public static void WriteLineElapsedTime(string label, long startTick, long stopTick, Logger logger = null)
		{
			var elapsedMillisecond = (stopTick - startTick) / (Stopwatch.Frequency / 1000.0);
			string output = String.Format("{0} : {1:F0} ms", label, elapsedMillisecond);

			if (logger == null)
				Debug.WriteLine(output);
			else
				logger.Info(output);
		}

		/// <summary>
		/// 시작시점과 종료시점에 소요된 시간 ms 단위로 반환한다.
		/// </summary>
		/// <param name="startTick">시작시점 Tick 값</param>
		/// <param name="stopTick">종료시점 Tick 값</param>
		/// <returns></returns>
		public static double GetElapsedTime(long startTick, long stopTick)
		{
			return (stopTick - startTick) / (Stopwatch.Frequency / 1000.0);
		}

		#endregion
	}
}
