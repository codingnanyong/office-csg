using System;
using CSG.MI.TrMontrgSrv.Helpers;
using CSG.MI.TrMontrgSrv.Helpers.Extentions;
using CSG.MI.TrMontrgSrv.LoggerService;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Core
{
    /// <summary>
    /// 로그 관련 처리 클래스.
    /// </summary>
    public static class LogHandler
    {
        #region Fields

        public enum TimeUnit { Millisecond, Second };

        private static readonly ILoggerManager _logger = new LoggerManager();

        #endregion

        #region Public Methods

        public static void Info(string deviceId, string msg)
        {
            _logger.LogInfo($"{deviceId} | {msg}");
        }

        public static void Warn(string deviceId, string msg)
        {
            _logger.LogWarn($"{deviceId} | {msg}");
        }

        public static void Error(string deviceId, string msg)
        {
            _logger.LogError($"{deviceId} | {msg}");
        }

        public static void Error(string deviceId, Exception ex)
        {
            _logger.LogError($"{deviceId} | {ex.ToFormattedString()}");
        }

        public static void Error(string deviceId, string msg, Exception ex)
        {
            _logger.LogError($"{deviceId} | {msg} : {ex.ToFormattedString()}");
        }

        public static void ElapsedTime(string deviceId, long startTick, long stopTick, TimeUnit unit = TimeUnit.Millisecond)
        {
            int timeElapsed = (int)DebugHelper.GetElapsedTime(startTick, stopTick);

            if (unit == TimeUnit.Millisecond)
            {
                Info(deviceId, $"Total elapsed time: {timeElapsed} ms.");
            }
            else if (unit == TimeUnit.Second)
            {
                double elapsedSecond = timeElapsed / 100;
                Info(deviceId, String.Format("Total elapsed time: {0:N2} s.", elapsedSecond));
            }
        }

        #endregion
    }
}
