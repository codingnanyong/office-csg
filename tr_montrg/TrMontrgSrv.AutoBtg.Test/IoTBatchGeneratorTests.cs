using CSG.MI.TrMontrgSrv.AutoBtg.Generator;
using CSG.MI.TrMontrgSrv.AutoBtg.Mock.Interface;
using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using Moq;

namespace CSG.MI.TrMontrgSrv.AutoBtg.Test
{
    [TestClass]
    public class IoTBatchGeneratorTests
    {
        #region Test Init

        private IoTBatchGenerator _generator = new IoTBatchGenerator();
        private InspecDevice _device = new InspecDevice();
        private IoTBatchFile _file = new IoTBatchFile();

        [TestInitialize]
        public void TestInitialize()
        {
            _device.DeviceId = "TestDevice";
            _device.IpAddress = "127.0.0.1";
            _file.Name = "IoTBat";
            _file.fileType = 0;
            _file.SetFileName();
        }

        #endregion

        [TestMethod]
        public void CreateBatchFileTest()
        {
            _generator.CreateBatch();

            string filePath = Path.Combine(Environment.CurrentDirectory, _file.FileFullName);
            Assert.IsTrue(File.Exists(filePath));
        }

        [TestMethod]
        public void WriteToBatchFileTest()
        {
            _generator.CreateBatch();
            _generator.WriteBatch(_device);

            string filePath = Path.Combine(Environment.CurrentDirectory, _file.FileFullName);
            string fileContent = File.ReadAllText(filePath);
            Assert.IsTrue(fileContent.Contains($"plink iotsys@{_device.IpAddress} -pw iot123sys -ssh -no-antispoof \"sudo reboot\""));
        }

        [TestMethod]
        public void DeleteToBatchFileTest()
        {
            _generator.CreateBatch();

            _generator.DeleteBatch();

            string filePath = Path.Combine(Environment.CurrentDirectory, _file.FileFullName);
            Assert.IsFalse(File.Exists(filePath));
        }

        [TestMethod]
        public void ExecutionBatchTest()
        {
            _generator.CreateBatch();
            _generator.WriteBatch(_device);

            var mockProcess = new Mock<IProcessWrapper>();

            _generator.SetMockProcess(mockProcess.Object);

            _generator.ExecutionBatch();

            mockProcess.Verify(p => p.WaitForExit(), Times.Once);
        }

        [TestMethod]
        public void AdminExecutionBatchTest()
        {
            _generator.CreateBatch();
            _generator.WriteBatch(_device);

            var mockProcess = new Mock<IProcessWrapper>();

            _generator.SetMockProcess(mockProcess.Object);
            _generator.AdminExecutionBatch();

            mockProcess.Verify(p => p.WaitForExit(), Times.Once);
        }

        [TestCleanup]
        public void Cleanup()
        {
            string[] files = Directory.GetFiles(Environment.CurrentDirectory, "*.bat");
            foreach (var file in files)
            {
                File.Delete(file);
            }
        }
    }
}
