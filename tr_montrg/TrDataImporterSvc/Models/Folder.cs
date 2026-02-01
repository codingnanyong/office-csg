using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using CSG.MI.TrMontrgSrv.TrDataImporterSvc.Core;
using CSG.MI.TrMontrgSrv.TrDataImporterSvc.Fsw;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Models
{
    public class Folder
    {
        #region Fields

        //private static readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private FileSystemWatcherEx _watcher;

        private System.Timers.Timer _folderWaitingTimer;
        private const double FOLDER_WAITING_TIMER_INTERVAL_SEC = 10;

        private bool _isFileProcessing = false;
        //private const int PROCESS_EXISTING_FILE_INTERVAL_SEC = 5;

        private DateTime _lastFileProcessedTime = DateTime.Now;
        private bool _isPrecessingExistingFiles = false;

        private string _fullPath;
        private string _filter;

        #endregion

        #region Constructors

        public Folder()
        {
            Filter = "*.*";
            State = FolderState.NotMonitoring;
        }

        #endregion

        #region Properties

        public FolderState State { get; private set; }

        public string FolderName => String.IsNullOrEmpty(FullPath) ? String.Empty : new DirectoryInfo(FullPath).Name;

        public string FullPath
        {
            get => _fullPath;
            set => _fullPath = value;
        }

        public string Filter
        {
            get => _filter;
            set => _filter = value;
        }

        public DateTime LastFileProcessedTime
        {
            get => _lastFileProcessedTime;
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// 모니터링 시작
        /// </summary>
        public void Start()
        {
            if (State == FolderState.Monitoring)
                return;

            //if (DirectoryHelper.Exists(FullPath) == false)
            //    DirectoryHelper.Create(FullPath);

            LoadWatcher();
            LoadFolderWaitingTimer();

            //throw new Exception("Test Exception");
        }

        /// <summary>
        /// 모니터링 종료
        /// </summary>
        public void Stop()
        {
            if (State != FolderState.Monitoring)
                return;

            UnloadWatcher();
        }

        /// <summary>
        /// 경로 휴효성 확인
        /// </summary>
        /// <returns>유효하면 <c>True</c>, 그렇지 않으면 <c>False</c></returns>
        public bool IsPathAvailable()
        {
            if (String.IsNullOrWhiteSpace(FullPath))
                return false;

            return Utility.IsPathAvailable(FullPath);
        }

        public Folder ShallowClone()
        {
            return (Folder)this.MemberwiseClone();
        }

        #endregion

        #region Event Handlers

        private void Watcher_Created(object sender, FileSystemEventArgs e)
        {
            while (_isFileProcessing)
                System.Threading.Thread.Sleep(200);

            // In case Windows event raises even before the file is closed by whichever process is still creating it
            System.Threading.Thread.Sleep(1000);
            ProcessFile(e.FullPath);
        }

        // 열화상 IoT장치가 원격지에서 HQ FDW서버의 FTP에 파일 업로드시 *.tmp 파일을 생성 후 *.zip 로 확장자명을 변경함.
        // 그렇기 때문에 파일 RENAME 이벤트도 추가적으로 감지를 해야 함.
        private void Watcher_Renamed(object sender, RenamedEventArgs e)
        {
            while (_isFileProcessing)
                System.Threading.Thread.Sleep(200);

            string newPath = e.FullPath;
            //string oldPath = e.OldFullPath;

            if (Path.GetExtension(newPath) == ".zip")
            {
                // In case Windows event raises even before the file is closed by whichever process is still creating it
                System.Threading.Thread.Sleep(1000);
                ProcessFile(newPath);
            }
        }

        private void Watcher_PathAvailabilityChanged(object sender, PathAvailablitiyEventArgs e)
        {
            if (e.PathIsAvailable)
            {
                State = FolderState.Monitoring;
                LogHandler.Info(FolderName, $"{FullPath} | State: {State}");

                _lastFileProcessedTime = DateTime.Now;
                ProcessExistingFile();
            }
            else
            {
                State = FolderState.PathNotAvailable;
                LogHandler.Warn(FolderName, $"{FullPath} | State: {State}");
            }
        }

        private void FolderWaitingTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            //_logger.Info($"{FullPath} | Checking path availability....");

            // 모니터링이 시작되면 타이머 중지
            if (State == FolderState.Monitoring)
            {
                _folderWaitingTimer.Stop();
            }
            else
            {
                LoadWatcher();
            }
        }

        #endregion

        #region Private Methods

        private void LoadWatcher()
        {
            if (IsPathAvailable())
            {
                UnloadWatcher();

                _watcher = new FileSystemWatcherEx(FullPath)
                {
                    Filter = Filter,
                    NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName,
                    EnableRaisingEvents = true,
                    InternalBufferSize = 64 * 1024 // Buffer size of a FileSystemWatcher in bytes (available range is 4 to 64 KBytes)
                };
                _watcher.Created += Watcher_Created;
                _watcher.Renamed += Watcher_Renamed;
                _watcher.PathAvailabilityChanged += Watcher_PathAvailabilityChanged;
                _watcher.StartFolderMonitor();
                State = FolderState.Monitoring;
                LogHandler.Info(FolderName, $"{FullPath} | Watcher is loaded. State: {State}");

                if (_folderWaitingTimer != null)
                    _folderWaitingTimer.Stop();

                ProcessExistingFile();
            }
            else
            {
                State = FolderState.PathNotAvailable;
                LogHandler.Info(FolderName, $"{FullPath} | State: {State}");
            }
        }

        private void UnloadWatcher()
        {
            if (_watcher == null)
                return;

            _watcher.StopFolderMonitor();
            _watcher.EnableRaisingEvents = false;
            _watcher.Created -= Watcher_Created;
            _watcher.PathAvailabilityChanged -= Watcher_PathAvailabilityChanged;
            _watcher.Dispose();
            _watcher = null;
            State = FolderState.NotMonitoring;
            LogHandler.Info(FolderName, $"{FullPath} | Watcher is unloaded. State: {State}");
        }

        private void LoadFolderWaitingTimer()
        {
            if (_folderWaitingTimer == null)
            {
                _folderWaitingTimer = new System.Timers.Timer();
                _folderWaitingTimer.Elapsed += FolderWaitingTimer_Elapsed;
                _folderWaitingTimer.Interval = TimeSpan.FromSeconds(FOLDER_WAITING_TIMER_INTERVAL_SEC).TotalMilliseconds;
                _folderWaitingTimer.Start();
            }
        }

        private void ProcessExistingFile()
        {
            if (_isPrecessingExistingFiles)
                return;

            try
            {
                _isPrecessingExistingFiles = true;

                var files = Directory.EnumerateFiles(FullPath, Filter);

                int total = files.Count();
                if (total > 0)
                {
                    LogHandler.Info(FolderName, $"{FullPath} | Found existing {total} files.");
                }

                foreach (var file in files)
                {
                    while (_isFileProcessing)
                        System.Threading.Thread.Sleep(200);

                    ProcessFile(file);

                    // 잠시 대기한다.
                    //System.Threading.Thread.Sleep(PROCESS_EXISTING_FILE_INTERVAL_SEC * 1000);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                _isPrecessingExistingFiles = false;
            }
        }

        private void ProcessFile(string file)
        {
            DbHandlerResult result = DbHandlerResult.Unknown;
            Stopwatch stopwatch = new();

            try
            {
                stopwatch.Start();
                result = DbHandler.Process(file);
            }
            catch (Exception ex)
            {
                LogHandler.Error(FolderName, ex);
            }
            finally
            {
                stopwatch.Stop();

                if (result == DbHandlerResult.Success)
                    LogHandler.Info(FolderName, $"{Path.GetFileName(file)} : {result} : {stopwatch.ElapsedMilliseconds} ms");
                else
                    LogHandler.Warn(FolderName, $"{Path.GetFileName(file)} : {result} : {stopwatch.ElapsedMilliseconds} ms");

                _isFileProcessing = false;
                _lastFileProcessedTime = DateTime.Now;
            }
        }

        #endregion
    }
}
