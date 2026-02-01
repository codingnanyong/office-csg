using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Production.DataMart
{
    [TestClass]
    public class CoordinateControllerTest
    {
        #region Test Set-Up

        private Mock<IWsCoordinateRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private CoordinateController _controller = null!;

        private List<SampleCoordinate> GetExpectedCoordinates()
        {
            return new List<SampleCoordinate>()
            {
                new SampleCoordinate() { tagId = "fb1226bbb06a"},
                new SampleCoordinate() { tagId = "fb1226bbb241"},
                new SampleCoordinate() { tagId = "fb1226bbb10e"}
            };
        }

        protected const string expectedOkCode = "s001";
        protected const string expectedFailCode = "XXX";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IWsCoordinateRepo>();
            _mockLogger = new Mock<ILoggerManager>();

            _controller = new CoordinateController(_mockRepo.Object, _mockLogger.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger = null!;
            _controller = null!;
        }

        #endregion

        #region Sample Coordinates Test

        [TestMethod]
        public void GetAll_ReturnsOkResult()
        {
            // Arrange
            var expectedCoordinates = GetExpectedCoordinates();
            _mockRepo.Setup(repo => repo.GetCoordinates())
                     .Returns(expectedCoordinates);

            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedCoordinates, okResult.Value);
        }

        [TestMethod]
        public void GetAll_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetCoordinates())
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
            var expectedCoordinates = GetExpectedCoordinates();
            _mockRepo.Setup(repo => repo.GetCoordinatesAsync())
                     .ReturnsAsync(expectedCoordinates);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedCoordinates, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetCoordinatesAsync())
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion

        #region Sample Coordinates History Test

        [TestMethod]
        public void GetHistories_ReturnsOkResult()
        {
            // Arrange
            var expectedHistories = new List<SampleCoordinateHistory>();
            _mockRepo.Setup(repo => repo.GetHistory())
                     .Returns(expectedHistories);

            // Act
            var result = _controller.GetHistories();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedHistories, okResult.Value);
        }

        [TestMethod]
        public void GetHistories_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetHistory())
                     .Returns(() => null!);

            // Act
            var result = _controller.GetHistories();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetHistoriesAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedHistories = new List<SampleCoordinateHistory>();
            _mockRepo.Setup(repo => repo.GetHistoryAsync())
                     .ReturnsAsync(expectedHistories);

            // Act
            var result = await _controller.GetHistoriesAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedHistories, okResult.Value);
        }

        [TestMethod]
        public async Task GetHistoriesAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetHistoryAsync())
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetHistoriesAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetHistoryBy_ReturnsOkResult()
        {
            // Arrange
            var expectedHistories = new List<SampleCoordinateHistory>();
            _mockRepo.Setup(repo => repo.GetHistoryBy(""))
                     .Returns(expectedHistories);

            // Act
            var result = _controller.GetHistoriesBy("");

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedHistories, okResult.Value);
        }

        [TestMethod]
        public void GetHistoryBy_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetHistoryBy(""))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetHistoriesBy("");

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetHistoryByAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedHistories = new List<SampleCoordinateHistory>();
            _mockRepo.Setup(repo => repo.GetHistoryByAsync(""))
                     .ReturnsAsync(expectedHistories);

            // Act
            var result = await _controller.GetHistoriesByAsync("");

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedHistories, okResult.Value);
        }

        [TestMethod]
        public async Task GetHistoryByAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetHistoryByAsync(""))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetHistoriesByAsync("");

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion
    }
}
