using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Production.DataMart
{
    [TestClass]
    public class FactoryTotalControllerTest
    {
        #region Test Set-Up

        private Mock<IFactoryPlnTotalRepo> _mockPlnRepo = null!;
        private Mock<IFactoryPrfTotalRepo> _mockPrfRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private FactoryTotalController _controller = null!;

        private List<FactoryTotal> GetExpectedFactoryTotal()
        {
            return new List<FactoryTotal>()
            {
                new FactoryTotal() { Year = "2022", Factory = "DS"},
                new FactoryTotal() { Year = "2023", Factory = "DS"},
                new FactoryTotal() { Year = "2024", Factory = "DS"}
            };
        }

        protected const string expectedFactory = "DS";
        protected const string expectedOkCode = "2022";

        [TestInitialize]
        public void Setup()
        {
            _mockPlnRepo = new Mock<IFactoryPlnTotalRepo>();
            _mockPrfRepo = new Mock<IFactoryPrfTotalRepo>();
            _mockLogger = new Mock<ILoggerManager>();

            _controller = new FactoryTotalController(_mockPlnRepo.Object, _mockPrfRepo.Object, _mockLogger.Object);
        }

        [TestCleanup]
        public void Cleanup() 
        {
            _mockPlnRepo = null!;
            _mockPrfRepo = null!;
            _mockLogger = null!;
            _controller = null!;
        }

        #endregion

        #region Pln-Total Test

        [TestMethod]
        public void GetPlns_ReturnsOkResult()
        {
            // Arrange
            var expectedPlans = GetExpectedFactoryTotal();
            _mockPlnRepo.Setup(repo => repo.GetPlans())
                        .Returns(expectedPlans);

            // Act
            var result = _controller.GetPlns();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPlans, okResult.Value);
        }

        [TestMethod]
        public void GetPlns_NotFoundResult()
        {
            // Arrange
            _mockPlnRepo.Setup(repo => repo.GetPlans())
                        .Returns(() => null!);

            // Act
            var result = _controller.GetPlns();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetPlnsAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedPlans = GetExpectedFactoryTotal();
            _mockPlnRepo.Setup(repo => repo.GetPlansAsync())
                        .ReturnsAsync(expectedPlans);

            // Act
            var result = await _controller.GetPlnsAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPlans, okResult.Value);
        }

        [TestMethod]
        public async Task GetPlnsAsync_NotFoundResult()
        {
            // Arrange
            _mockPlnRepo.Setup(repo => repo.GetPlansAsync())
                        .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetPlnsAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetPlnByFactory_ReturnsOkResult()
        {
            // Arrange
            var expectedPlans = GetExpectedFactoryTotal().Where(x => x.Factory == expectedFactory)
                                                         .ToList();
            _mockPlnRepo.Setup(repo => repo.GetByFactory(expectedFactory))
                        .Returns(expectedPlans);

            // Act
            var result = _controller.GetPlnByFactory(expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPlans, okResult.Value);
        }

        [TestMethod]
        public void GetPlnByFactory_NotFoundResult()
        {
            // Arrange
            _mockPlnRepo.Setup(repo => repo.GetByFactory(expectedFactory))
                        .Returns(() => null!);

            // Act
            var result = _controller.GetPlnByFactory(expectedFactory);


            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetPlnByFactoryAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedPlans = GetExpectedFactoryTotal();
            _mockPlnRepo.Setup(repo => repo.GetByFactoryAsync(expectedFactory))
                        .ReturnsAsync(expectedPlans);

            // Act
            var result = await _controller.GetPlnByFactoryAsync(expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPlans, okResult.Value);
        }

        [TestMethod]
        public async Task GetPlnByFactoryAsync_NotFoundResult()
        {
            // Arrange
            _mockPlnRepo.Setup(repo => repo.GetByFactoryAsync(expectedFactory))
                        .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetPlnByFactoryAsync(expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetPlnByFactoryToYear_ReturnsOkResult()
        {
            // Arrange
            var expectedPlans = GetExpectedFactoryTotal().Where(x => x.Factory == expectedFactory && x.Year == expectedOkCode)
                                                         .FirstOrDefault();
            _mockPlnRepo.Setup(repo => repo.GetByYearFactory(expectedOkCode,expectedFactory))
                        .Returns(expectedPlans!);

            // Act
            var result = _controller.GetPlnByFactoryToYear(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPlans, okResult.Value);
        }

        [TestMethod]
        public void GetPlnByFactoryToYear_NotFoundResult()
        {
            // Arrange
            _mockPlnRepo.Setup(repo => repo.GetByYearFactory(expectedOkCode, expectedFactory))
                        .Returns(() => null!);

            // Act
            var result = _controller.GetPlnByFactoryToYear(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetPlnByFactoryToYearAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedPlans = GetExpectedFactoryTotal().Where(x => x.Factory == expectedFactory && x.Year == expectedOkCode)
                                                         .FirstOrDefault();
            _mockPlnRepo.Setup(repo => repo.GetByYearFactoryAsync(expectedOkCode, expectedFactory))
                        .ReturnsAsync(expectedPlans!);

            // Act
            var result = await _controller.GetPlnByFactoryToYearAsync(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPlans, okResult.Value);
        }

        [TestMethod]
        public async Task GetPlnByFactoryToYearAsync_NotFoundResult()
        {
            // Arrange
            _mockPlnRepo.Setup(repo => repo.GetByYearFactoryAsync(expectedOkCode, expectedFactory))
                        .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetPlnByFactoryToYearAsync(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion

        #region Prf-Total Test

        [TestMethod]
        public void GetPrfs_ReturnsOkResult()
        {
            // Arrange
            var expectedPrfs = GetExpectedFactoryTotal();
            _mockPrfRepo.Setup(repo => repo.GetPerformaces())
                        .Returns(expectedPrfs);

            // Act
            var result = _controller.GetPrfs();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPrfs, okResult.Value);
        }

        [TestMethod]
        public void GetPrfs_NotFoundResult()
        {
            // Arrange
            _mockPrfRepo.Setup(repo => repo.GetPerformaces())
                        .Returns(() => null!);

            // Act
            var result = _controller.GetPrfs();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetPrfsAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedPrfs = GetExpectedFactoryTotal();
            _mockPrfRepo.Setup(repo => repo.GetPerformacesAsync())
                        .ReturnsAsync(expectedPrfs);

            // Act
            var result = await _controller.GetPrfsAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPrfs, okResult.Value);
        }

        [TestMethod]
        public async Task GetPrfsAsync_NotFoundResult()
        {
            // Arrange
            _mockPrfRepo.Setup(repo => repo.GetPerformacesAsync())
                        .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetPrfsAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetPrfByFactory_ReturnsOkResult()
        {
            // Arrange
            var expectedPrfs = GetExpectedFactoryTotal().Where(x => x.Factory == expectedFactory)
                                                        .ToList();
            _mockPrfRepo.Setup(repo => repo.GetByFactory(expectedFactory))
                        .Returns(expectedPrfs);

            // Act
            var result = _controller.GetPrfByFactory(expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPrfs, okResult.Value);
        }

        [TestMethod]
        public void GetPrfByFactory_NotFoundResult()
        {
            // Arrange
            _mockPrfRepo.Setup(repo => repo.GetByFactory(expectedFactory))
                        .Returns(() => null!);

            // Act
            var result = _controller.GetPrfByFactory(expectedFactory);


            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetPrfByFactoryAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedPrfs = GetExpectedFactoryTotal();
            _mockPrfRepo.Setup(repo => repo.GetByFactoryAsync(expectedFactory))
                        .ReturnsAsync(expectedPrfs);

            // Act
            var result = await _controller.GetPrfByFactoryAsync(expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPrfs, okResult.Value);
        }

        [TestMethod]
        public async Task GetPrfByFactoryAsync_NotFoundResult()
        {
            // Arrange
            _mockPrfRepo.Setup(repo => repo.GetByFactoryAsync(expectedFactory))
                        .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetPrfByFactoryAsync(expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetPrfByFactoryToYear_ReturnsOkResult()
        {
            // Arrange
            var expectedPrfs = GetExpectedFactoryTotal().Where(x => x.Factory == expectedFactory && x.Year == expectedOkCode)
                                                         .FirstOrDefault();
            _mockPrfRepo.Setup(repo => repo.GetByYearFactory(expectedOkCode, expectedFactory))
                        .Returns(expectedPrfs!);

            // Act
            var result = _controller.GetPrfByFactoryToYear(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPrfs, okResult.Value);
        }

        [TestMethod]
        public void GetPrfByFactoryToYear_NotFoundResult()
        {
            // Arrange
            _mockPrfRepo.Setup(repo => repo.GetByYearFactory(expectedOkCode, expectedFactory))
                        .Returns(() => null!);

            // Act
            var result = _controller.GetPrfByFactoryToYear(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetPrfByFactoryToYearAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedPrfs = GetExpectedFactoryTotal().Where(x => x.Factory == expectedFactory && x.Year == expectedOkCode)
                                                         .FirstOrDefault();
            _mockPrfRepo.Setup(repo => repo.GetByYearFactoryAsync(expectedOkCode, expectedFactory))
                        .ReturnsAsync(expectedPrfs!);

            // Act
            var result = await _controller.GetPrfByFactoryToYearAsync(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedPrfs, okResult.Value);
        }

        [TestMethod]
        public async Task GetPrfByFactoryToYearAsync_NotFoundResult()
        {
            // Arrange
            _mockPrfRepo.Setup(repo => repo.GetByYearFactoryAsync(expectedOkCode, expectedFactory))
                        .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetPrfByFactoryToYearAsync(expectedOkCode, expectedFactory);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion
    }
}
