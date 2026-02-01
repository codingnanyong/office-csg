using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Fsw
{
    /// <summary>
    /// FileSystemWatcher 확장 클래스.
    /// </summary>
    public class FileSystemWatcherEx : FileSystemWatcher
    {
        #region Events

        /// <summary>
        /// 폴더 경로 유효 이벤트
        /// </summary>
        public event PathAvailabilityEventHandler PathAvailabilityChanged = delegate { };

        #endregion

        #region Fields

        private const int MAX_INTERVAL = 60000;         // 경로 검사간격 최대 1분
        private string _name = "FileSystemWatcherEx";   // 기본 파일 시스템 와쳐명
        private bool _isNetworkAvailable = true;
        private int _interval = 100;                    // 기본 검사간격
        private Thread _thread = null;                  // 경로검사를 위한 모니터링 쓰레드
        private bool _running = false;
        private readonly ManualResetEvent _waitHandler = new (false);

        #endregion // Fields

        #region Constructors

        /// <summary>
        /// 기본 생성자
        /// </summary>
        public FileSystemWatcherEx() : base()
        {
            CreateThread();
        }

        /// <summary>
        /// 모니터할 경로 인자를 동반한 생성자
        /// </summary>
        /// <param name="path">모니터할 경로</param>
        public FileSystemWatcherEx(string path) : base(path)
        {
            CreateThread();
        }

        /// <summary>
        /// 경로 유효성 검사 시간간격 인자를 동반한 생성자
        /// </summary>
        /// <param name="interval">경로 유효성 검사 시간간격</param>
        public FileSystemWatcherEx(int interval) : base()
        {
            _interval = interval;
            CreateThread();
        }

        /// <summary>
        /// 경로와 시간간격 인자를 동반한 생성자
        /// </summary>
        /// <param name="path">모니터할 경로</param>
        /// <param name="interval">경로 유효성 검사 시간간격</param>
        public FileSystemWatcherEx(string path, int interval) : base(path)
        {
            _interval = interval;
            CreateThread();
        }

        /// <summary>
        /// 시간간격과 이름 인자를 동반한 생성자
        /// </summary>
        /// <param name="interval">경로 유효성 검사 시간간격</param>
        /// <param name="name">와쳐명</param>
        public FileSystemWatcherEx(int interval, string name) : base()
        {
            _interval = interval;
            _name = name;
            CreateThread();
        }

        /// <summary>
        /// 경로, 시간간격, 이름 인자들을 동반한 생성자
        /// </summary>
        /// <param name="path">모니터할 경로</param>
        /// <param name="interval">경로 유효성 검사 시간간격</param>
        /// <param name="name">와쳐명</param>
        public FileSystemWatcherEx(string path, int interval, string name) : base(path)
        {
            _interval = interval;
            _name = name;
            CreateThread();
        }

        #endregion // Constructors

        #region Properties

        /// <summary>
        /// 파일 시스템 와쳐명
        /// </summary>
        public string Name
        {
            get => _name;
            set => _name = value;
        }

        /// <summary>
        /// 경로 유효성 검사 시간간격
        /// </summary>
        public int MaxInterval => MAX_INTERVAL;


        #endregion

        #region Public Methods

        /// <summary>
        /// 쓰레드 메소드로써, 폴더 경로 존재여부를 확인하기 위해 반복회전 검사한다.
        /// </summary>
        public void MonitorFolderAvailability()
        {
            while (_running)
            {
                if (_isNetworkAvailable)
                {
                    if (Utility.IsPathAvailable(base.Path) == false)
                    {
                        _isNetworkAvailable = false;
                        RaiseEventNetworkPathAvailability();
                    }
                }
                else
                {
                    if (Utility.IsPathAvailable(base.Path))
                    {
                        _isNetworkAvailable = true;
                        RaiseEventNetworkPathAvailability();
                    }
                }

                _waitHandler.WaitOne(); // 신호가 올때까지 현재 쓰레드 정지

                Thread.Sleep(_interval);
            }
        }

        /// <summary>
        /// 모니터링 쓰레드를 시작한다.
        /// </summary>
        public void StartFolderMonitor()
        {
            _running = true;

            if (_thread != null)
            {
                if (_thread.ThreadState != (ThreadState.Background | ThreadState.WaitSleepJoin) &&
                     _thread.ThreadState != (ThreadState.Background | ThreadState.Running))
                {
                    _thread.Start();
                }

                _waitHandler.Set(); // 대기중인 쓰레드들에게 진행 신호
            }
        }

        /// <summary>
        /// 모니터링 쓰레드를 정지한다.
        /// </summary>
        public void StopFolderMonitor()
        {
            _running = false;
        }

        /// <summary>
        /// 모니터링 쓰레드를 잠시 중지한다.
        /// </summary>
        public void PauseFolderMonitor()
        {
            _waitHandler.Reset(); // 실행중인 쓰레드 중지 신호
        }

        /// <summary>
        /// 중지된 모니터링 쓰레드를 다시 진행한다.
        /// </summary>
        public void ContinueFolderMonitor()
        {
            _waitHandler.Set(); // 대기중인 쓰레드들에게 진행 신호
        }

        #endregion // Public Methods

        #region Private Methods

        /// <summary>
        /// 만일 시간간격이 0 millisecond 보다 크면, 쓰레드를 생성한다.
        /// </summary>
        private void CreateThread()
        {
            // Normalize the interval
            _interval = Math.Max(0, Math.Min(_interval, MAX_INTERVAL));
            // If the interval is 0, this indicates we don't want to monitor the path for availability
            if (_interval > 0)
            {
                _thread = new Thread(new ThreadStart(MonitorFolderAvailability))
                {
                    Name = Name,
                    IsBackground = true
                };
            }
        }

        /// <summary>
        /// 경로 유효 모니터를 위해 이벤트를 발생시킨다.
        /// </summary>
        private void RaiseEventNetworkPathAvailability()
        {
            PathAvailabilityChanged(this, new PathAvailablitiyEventArgs(_isNetworkAvailable));
        }

        #endregion
    }
}
