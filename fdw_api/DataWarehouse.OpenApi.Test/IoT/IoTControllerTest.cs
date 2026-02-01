using CSG.MI.DTO.IoT;
using CSG.MI.FDW.BLL.IoT.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Hosting;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.IoT
{
    [TestClass]
    public class IoTControllerTest
    {
        #region Test Set-Up

        private Mock<IEnviromentRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private IoTController _controller = null!;

        protected const int expectedOkLocationCode = 1;
        protected const int expectedFailLocationCode = 3;

        private DeviceInfo GetExpectedEnviromentData()
        {
            return new DeviceInfo
            {
                Location = "3D Printer Room",
                Sensors = new List<SensorInfo> {
                    new SensorInfo {
                        SensorId = "TEMPIOT-A001",
                        Environments = new List<EnvironmentData> {
                            new EnvironmentData
                            {
                                MeasureTime = DateTime.Now,
                                Temperature = 22.5f,
                                Humidity = 55.0f,
                            }
                        }
                    }
                }
            };
        }

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IEnviromentRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _controller = new IoTController(_mockRepo.Object, _mockLogger.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger = null!;
            _controller = null!;
        }

        #endregion

        [TestMethod]
        public void GetNow_OkResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironment_Now(expectedOkLocationCode))
                     .Returns(expectedEnviromentData);

            // Act
            var result = _controller.GetNow(expectedOkLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnviromentData, okResult.Value);
        }

        [TestMethod]
        public void GetNow_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetLocationEnvironment_Now(expectedFailLocationCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetNow(expectedFailLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetNowAsync_OkResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Now(expectedOkLocationCode))
                     .ReturnsAsync(expectedEnviromentData);

            // Act
            var result = await _controller.GetNowAsync(expectedOkLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnviromentData, okResult.Value);
        }

        [TestMethod]
        public async Task GetNowAsync_NotFoundResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Now(expectedFailLocationCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetNowAsync(expectedFailLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetToday_OkResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironment_Today(expectedOkLocationCode))
                     .Returns(expectedEnviromentData);

            // Act
            var result = _controller.GetToday(expectedOkLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnviromentData, okResult.Value);
        }

        [TestMethod]
        public void GetToday_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetLocationEnvironment_Today(expectedFailLocationCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetToday(expectedFailLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetTodayAsync_OkResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Today(expectedOkLocationCode))
                     .ReturnsAsync(expectedEnviromentData);

            // Act
            var result = await _controller.GetTodayAsync(expectedOkLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnviromentData, okResult.Value);
        }

        [TestMethod]
        public async Task GetTodayAsync_NotFoundResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Today(expectedFailLocationCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetTodayAsync(expectedFailLocationCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetRange_ValidRequest_ReturnsOkResult()
        {
            // Arrange
            string start = "20240815";
            string end = "20240816";
            var expectedEnvironments = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironment_Range(expectedOkLocationCode,start, end))
                     .Returns(expectedEnvironments);

            // Act
            var result = _controller.GetRange(expectedOkLocationCode, start, end);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnvironments, okResult.Value);
        }

        [TestMethod]
        public void GetRange_EndDateIsEmpty_ReturnsOkResult()
        {
            // Arrange
            string start = "20240815";
            string end = "";
            var expectedEnvironments = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironment_Range(expectedOkLocationCode, start, end))
                     .Returns(expectedEnvironments);

            // Act
            var result = _controller.GetRange(expectedOkLocationCode, start);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnvironments, okResult.Value);
        }

        [TestMethod]
        public void GetRange_StartDateIsNull_ReturnsBadRequest()
        {
            // Arrange
            string start = null!;
            string end = "20240709";

            // Act
            var result = _controller.GetRange(expectedOkLocationCode, start, end);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public void GetRange_NotFoundResult()
        {
            // Arrange
            string start = "20240815";
            string end = "";
            _mockRepo.Setup(repo => repo.GetLocationEnvironment_Range(expectedFailLocationCode, start, end))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetRange(expectedFailLocationCode, start, end);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetRangeAsync_ValidRequest_ReturnsOkResult()
        {
            // Arrange
            string start = "20240815";
            string end = "20240816";
            var expectedEnvironments = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Range(expectedOkLocationCode, start, end))
                     .ReturnsAsync(expectedEnvironments);

            // Act
            var result = await _controller.GetRangeAsync(expectedOkLocationCode, start, end);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnvironments, okResult.Value);
        }

        [TestMethod]
        public async Task GetRangeAsync_EndDateIsEmpty_ReturnsOkResult()
        {
            // Arrange
            string start = "20240816";
            string end = "";
            var expectedEnvironments = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Range(expectedOkLocationCode, start, end))
                     .ReturnsAsync(expectedEnvironments);

            // Act
            var result = await _controller.GetRangeAsync(expectedOkLocationCode, start, end);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnvironments, okResult.Value);
        }

        [TestMethod]
        public async Task GetRangeAsync_StartDateIsNull_ReturnsBadRequest()
        {
            // Arrange
            string start = null!;
            string end = "20240709";
            var expectedEnvironments = GetExpectedEnviromentData();
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Range(expectedOkLocationCode,start, end))
                     .ReturnsAsync(expectedEnvironments);
            // Act
            var result = await _controller.GetRangeAsync(expectedOkLocationCode,start, end);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public async Task GetRangeAsync_NotFoundResult()
        {
            // Arrange
            string start = "20240815";
            string end = "";
            _mockRepo.Setup(repo => repo.GetLocationEnvironmentAsync_Range(expectedFailLocationCode,start, end))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetRangeAsync(expectedFailLocationCode,start, end);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }
    }
}
