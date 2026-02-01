using CSG.MI.DTO.Analysis;
using CSG.MI.FDW.BLL.Analysis.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Analysis
{
    [TestClass]
    public class PredictionControllerTest
    {
        private Mock<IDevStylePredictionRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private PredictionController _controller = null!;

        private List<DevStylePrediction> GetExpectedDevStylePrediction()
        {
            return new List<DevStylePrediction>()
            {
                new DevStylePrediction() { DevStyleNumber = "553558" },
                new DevStylePrediction() { DevStyleNumber = "CV0400" }
            };
        }

        protected const string expectedOkCode = "553558";
        protected const string expectedFailCode = "XXX";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IDevStylePredictionRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _controller = new PredictionController(_mockRepo.Object, _mockLogger.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger = null!;
            _controller = null!;
        }

        [TestMethod]
        public void GetAll_ReturnsOkResult()
        {
            // Arrange
            var expectedDevStyle = GetExpectedDevStylePrediction();
            _mockRepo.Setup(repo => repo.GetDevPredictions())
                     .Returns(expectedDevStyle);
            
            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDevStyle, okResult.Value);
        }

        [TestMethod]
        public void GetAll_ReturnsNotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDevPredictions())
                     .Returns(() => null!);

            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetAllAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedDevStyle = GetExpectedDevStylePrediction();
            _mockRepo.Setup(repo => repo.GetDevPredictionsAsync())
                     .ReturnsAsync(expectedDevStyle);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDevStyle, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_ReturnsNotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDevPredictionsAsync())
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void Get_ReturnsOkResult()
        {
            // Arrange
            var expectedDevStyle = GetExpectedDevStylePrediction().Where(x => x.DevStyleNumber == expectedOkCode)
                                                               .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetDevPrediction(expectedOkCode))
                     .Returns(expectedDevStyle!);

            // Act
            var result = _controller.Get(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDevStyle, okResult.Value);
        }

        [TestMethod]
        public void Get_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedDevStyle = GetExpectedDevStylePrediction().Where(x => x.DevStyleNumber == expectedFailCode)
                                                               .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetDevPrediction(expectedOkCode))
                     .Returns(expectedDevStyle!);

            // Act
            var result = _controller.Get(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedDevStyle = GetExpectedDevStylePrediction().Where(x => x.DevStyleNumber == expectedOkCode)
                                                               .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetDevPredictionAsync(expectedOkCode))
                     .ReturnsAsync(expectedDevStyle!);

            // Act
            var result = await _controller.GetAsync(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDevStyle, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedDevStyle = GetExpectedDevStylePrediction().Where(x => x.DevStyleNumber == expectedFailCode)
                                                               .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetDevPredictionAsync(expectedOkCode))
                     .ReturnsAsync(expectedDevStyle!);

            // Act
            var result = await _controller.GetAsync(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }
    }
}
