using CSG.MI.DAO.Production.RTLS;
using CSG.MI.FDW.BLL.Production.RTLS.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Production.RTLS
{
    [TestClass]
    public class LocationControllerTest
    {
        private Mock<IEslLocationRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<EslLocation>> _mockcsv = null!;
        private Mock<IJsonResultService<EslLocation>> _mockjson = null!;
        private Mock<IXmlResultService<EslLocation>> _mockxml = null!;
        private LocationController _controller = null!;

        private List<EslLocation> GetExpectedLocations()
        {
            return new List<EslLocation>()
            {
                new EslLocation() { BeaconTagId = "fb1226bbb001" },
                new EslLocation() { BeaconTagId = "fb1226bbb003" }
            };
        }

        protected const string expectedOkCode = "fb1226bbb001";
        protected const string expectedFailCode = "XXX";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IEslLocationRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockcsv = new Mock<ICsvResultService<EslLocation>>();
            _mockjson = new Mock<IJsonResultService<EslLocation>>();
            _mockxml = new Mock<IXmlResultService<EslLocation>>();
            _controller = new LocationController(_mockRepo.Object, _mockcsv.Object, _mockjson.Object, _mockxml.Object, _mockLogger.Object);
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
            var expectedLocations = GetExpectedLocations();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
                     .Returns(expectedLocations);

            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedLocations, okResult.Value);
        }

        [TestMethod]
        public void GetAll_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetCurrentAll())
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
            var expectedLocations = GetExpectedLocations();
            _mockRepo.Setup(repo => repo.GetCurrentAllAsync())
                     .ReturnsAsync(expectedLocations);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedLocations, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetCurrentAllAsync())
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetAllFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedLocations = GetExpectedLocations();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
                     .Returns(expectedLocations);
            _mockjson.Setup(service => service.Download(expectedLocations, "all_cur_loc.json"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetAllFile("json");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/json", fileResult.ContentType);
        }

        [TestMethod]
        public void GetAllFile_ReturnsCsvSuccess()
        {
            // Arrange
            var expectedLocations = GetExpectedLocations();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
                     .Returns(expectedLocations);
            _mockcsv.Setup(service => service.Download(expectedLocations, "all_cur_loc.csv"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetAllFile("csv");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("text/csv", fileResult.ContentType);
        }

        [TestMethod]
        public void GetAllFile_ReturnsXmlSuccess()
        {
            // Arrange
            var expectedLocations = GetExpectedLocations();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
                     .Returns(expectedLocations);
            _mockxml.Setup(service => service.Download(expectedLocations, "all_cur_loc.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetAllFile("xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }

        [TestMethod]
        public void Get_ReturnsOkResult()
        {
            // Arrange
            var expectedLocation = GetExpectedLocations().Where(x => x.BeaconTagId == expectedOkCode)
                                                         .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedLocation!);

            // Act
            var result = _controller.Get(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedLocation, okResult.Value);
        }

        [TestMethod]
        public void Get_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedLocation = GetExpectedLocations().Where(x => x.BeaconTagId == expectedFailCode)
                                                         .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedLocation!);

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
            var expectedLocation = GetExpectedLocations().Where(x => x.BeaconTagId == expectedOkCode)
                                                         .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrentAsync(expectedOkCode))
                     .ReturnsAsync(expectedLocation!);

            // Act
            var result = await _controller.GetAsync(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedLocation, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedLocation = GetExpectedLocations().Where(x => x.BeaconTagId == expectedFailCode)
                                                         .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrentAsync(expectedOkCode))
                     .ReturnsAsync(expectedLocation!);

            // Act
            var result = await _controller.GetAsync(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedLocation = GetExpectedLocations().Where(x => x.BeaconTagId == expectedOkCode)
                                                         .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedLocation!);
            _mockjson.Setup(service => service.Download(expectedLocation!, "tag_cur_loc.json"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode, "json");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/json", fileResult.ContentType);
        }

        [TestMethod]
        public void GetFile_ReturnsCsvSuccess()
        {
            // Arrange
            var expectedLocation = GetExpectedLocations().Where(x => x.BeaconTagId == expectedOkCode)
                                                         .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedLocation!);
            _mockcsv.Setup(service => service.Download(expectedLocation!, "tag_cur_loc.csv"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode, "csv");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("text/csv", fileResult.ContentType);
        }

        [TestMethod]
        public void GetFile_ReturnsXmlSuccess()
        {
            // Arrange
            var expectedLocation = GetExpectedLocations().Where(x => x.BeaconTagId == expectedOkCode)
                                                         .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedLocation!);
            _mockxml.Setup(service => service.Download(expectedLocation!, "tag_cur_loc.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode, "xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }
    }
}
