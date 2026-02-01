using CSG.MI.TrMontrgSrv.AutoBtg.Generator.Interface;
using CSG.MI.TrMontrgSrv.AutoBtg.Mock.Interface;
using CSG.MI.TrMontrgSrv.Model.Inspection;

namespace CSG.MI.TrMontrgSrv.AutoBtg.Generator
{
    public class IoTBatchGenerator : IBatchGenerator<InspcDevice>
    {
        #region Field

        private const string _file = "IoTBat";
        private string _command = $"plink iotsys@{{ip}} -pw iot123sys -ssh -no-antispoof \"sudo reboot\"";
        private const string WarimingComent = "Invalid device information. Unable to write to batch file.";

        private IoTBatchFile bat;
        private IProcessWrapper? _mockProcess;

        #endregion

        #region Constructor

        public IoTBatchGenerator()
        {
            bat = new IoTBatchFile();
            bat.Name = _file;
            bat.fileType = 0;
            bat.SetFileName();
            _mockProcess = null;
        }

        #endregion

        #region Public Methods - Batch File Create.Write.Run(Admin)

        /// <summary>
        /// Mock Test
        /// </summary>
        /// <param name="mockProcess">Mock Test Process</param>
        public void SetMockProcess(IProcessWrapper mockProcess)
        {
            _mockProcess = mockProcess;
        }

        /// <summary>
        /// Batch Runner
        /// </summary>
        /// <param name="iot">IoT Device Info.</param>
        public void Run(InspcDevice iot)
        {
            if (IsInvalidDevice(iot))
            {
                StaticLogger.Logger.LogWarn(WarimingComent);
                return;
            }

            if (!isFileExist())
                CreateBatch();

            WriteBatch(iot);
            AdminExecutionBatch();
        }

        /// <summary>
        /// Create Batch File.
        /// </summary>
        public void CreateBatch()
        {
            string FilePath = Path.Combine(Environment.CurrentDirectory, bat.FileFullName);

            try
            {
                File.WriteAllText(FilePath, string.Empty);
            }
            catch (Exception ex)
            {
                StaticLogger.Logger.LogError($"An error occurred: {ex.Message}");
            }
        }

        /// <summary>
        /// Write Batch File.
        /// </summary>
        /// <param name="iot">IoT Device Info. Use iot.IpAddress</param>
        public void WriteBatch(InspcDevice iot)
        {
            if (IsInvalidDevice(iot))
            {
                StaticLogger.Logger.LogWarn(WarimingComent);
                return;
            }

            bat.Command = _command.Replace("{ip}", iot.IpAddress);
            string FilePath = Path.Combine(Environment.CurrentDirectory, bat.FileFullName);

            try
            {
                File.AppendAllText(FilePath, bat.Command);
            }
            catch (Exception ex)
            {
                StaticLogger.Logger.LogError($"An error occurred: {ex.Message}");
            }
        }

        /// <summary>
        /// Delete Batch File.
        /// </summary>
        public void DeleteBatch()
        {
            string FilePath = Path.Combine(Environment.CurrentDirectory, bat.FileFullName);

            try
            {
                File.Delete(FilePath);
            }
            catch (Exception ex)
            {
                StaticLogger.Logger.LogError($"An error occurred while deleting batch file: {ex.Message}");
            }
        }

        /// <summary>
        /// Execurtion Batch File.
        /// </summary>
        public void ExecutionBatch()
        {
            string FilePath = Path.Combine(Environment.CurrentDirectory, bat.FileFullName);

            try
            {
                if (_mockProcess != null)
                {
                    _mockProcess.WaitForExit();
                    Console.WriteLine("HasExited is accessed in mockProcess");
                }
                else
                {
                    var startInfo = new System.Diagnostics.ProcessStartInfo
                    {
                        FileName = FilePath,
                        UseShellExecute = false,
                    };

                    System.Diagnostics.Process.Start(startInfo);
                }
            }
            catch (Exception ex)
            {
                StaticLogger.Logger.LogError($"An error occurred: {ex.Message}");
            }
        }

        /// <summary>
        /// Execurtion Batch File with Administrator Privileges.
        /// </summary>
        public void AdminExecutionBatch()
        {
            string FilePath = Path.Combine(Environment.CurrentDirectory, bat.FileFullName);

            try
            {
                if (_mockProcess != null)
                {
                    _mockProcess.WaitForExit();
                }
                else
                {
                    // Run as administrator
                    var startInfo = new System.Diagnostics.ProcessStartInfo
                    {
                        FileName = FilePath,
                        Verb = "runas",
                        UseShellExecute = false,
                    };

                    System.Diagnostics.Process.Start(startInfo);
                }
            }
            catch (Exception ex)
            {
                StaticLogger.Logger.LogError($"An error occurred: {ex.Message}");
            }
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Invalid Device
        /// </summary>
        /// <param name="inspcDevice"></param>
        /// <returns></returns>
        private bool IsInvalidDevice(InspcDevice inspcDevice)
        {
            return inspcDevice == null || string.IsNullOrWhiteSpace(inspcDevice.DeviceId);
        }

        /// <summary>
        /// File Exist Test.
        /// </summary>
        /// <returns></returns>
        public bool isFileExist()
        {
            if (File.Exists(bat.FileFullName))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        #endregion
    }
}
