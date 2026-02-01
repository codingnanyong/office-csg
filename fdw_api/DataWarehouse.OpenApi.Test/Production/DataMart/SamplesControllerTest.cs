using CSG.MI.DTO.Production;
using CSG.MI.FDW.BLL.Production.DataMart.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Production.DataMart
{
    [TestClass]
    public class SamplesControllerTest
    {
        private Mock<IWsSampleRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<Sample>> _mockcsv = null!;
        private Mock<IJsonResultService<Sample>> _mockjson = null!;
        private Mock<IXmlResultService<Sample>> _mockxml = null!;
        private SamplesController _controller = null!;

        private List<Sample> GetExpectedSamples()
        {
            return new List<Sample>()
            {
                new Sample() { Factory ="DS" , WsNo = "WS"},
            };
        }

        protected const string expectedFactoryCode = "DS";
        protected const string expectedOkCode = "WS";
        protected const string expectedFailCode = "xxx";
        protected const string expectedSrchCode = "zoom";
        protected const string expectedOpCode = "UPC";
        protected const string expectedStatusCode = "I";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IWsSampleRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockcsv = new Mock<ICsvResultService<Sample>>();
            _mockjson = new Mock<IJsonResultService<Sample>>();
            _mockxml = new Mock<IXmlResultService<Sample>>();

            _controller = new SamplesController(_mockRepo.Object, _mockLogger.Object, _mockcsv.Object, _mockjson.Object, _mockxml.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger = null!;
            _mockcsv = null!;
            _mockjson = null!;
            _mockxml = null!;
            _controller = null!;
        }

        [TestMethod]
        public void GetSamples_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            _mockRepo.Setup(repo => repo.GetSamples())
                     .Returns(expectedSamples);

            // Act
            var result = _controller.GetSamples();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public void GetSamples_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetSamples())
                     .Returns(() => null!);

            // Act
            var result = _controller.GetSamples();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetSamplesAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            _mockRepo.Setup(repo => repo.GetSamplesAsync())
                     .ReturnsAsync(expectedSamples);

            // Act
            var result = await _controller.GetSamplesAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public async Task GetSamplesAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetSamplesAsync())
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetSamplesAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetSamplesFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetSamples())
                     .Returns(expectedSamples);
            _mockjson.Setup(service => service.Download(expectedSamples, "samples.json"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetSamplesFile("json");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/json", fileResult.ContentType);
        }

        [TestMethod]
        public void GetSamplesFile_ReturnsCsvSuccess()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetSamples())
                     .Returns(expectedSamples);
            _mockcsv.Setup(service => service.Download(expectedSamples, "samples.csv"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetSamplesFile("csv");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("text/csv", fileResult.ContentType);
        }

        [TestMethod]
        public void GetSamplesFile_ReturnsXmlSuccess()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetSamples())
                     .Returns(expectedSamples);
            _mockxml.Setup(service => service.Download(expectedSamples, "samples.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetSamplesFile("xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }

        [TestMethod]
        public void GetSample_ReturnsOkResult()
        {
            // Arrange
            var expectedSample = GetExpectedSamples().Where(x => x.Factory == expectedFactoryCode && x.WsNo.Contains(expectedOkCode)).FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetSample(expectedOkCode,expectedFactoryCode))
                     .Returns(expectedSample!);

            // Act
            var result = _controller.GetSample(expectedOkCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSample, okResult.Value);
        }

        [TestMethod]
        public void GetSample_NotFoundResult()
        {
            // Arrange
            var expectedSample = GetExpectedSamples().Where(x => x.Factory == expectedFactoryCode && x.WsNo.Contains(expectedFailCode)).FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetSample(expectedOkCode, expectedFactoryCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetSample(expectedOkCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetSampleAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedSample = GetExpectedSamples().Where(x => x.Factory == expectedFactoryCode && x.WsNo.Contains(expectedOkCode)).FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetSampleAsync(expectedOkCode, expectedFactoryCode))
                     .ReturnsAsync(expectedSample!);

            // Act
            var result = await _controller.GetSampleAsync(expectedOkCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSample, okResult.Value);
        }

        [TestMethod]
        public async Task GetSampleAsync_NotFoundResult()
        {
            // Arrange
            var expectedSample = GetExpectedSamples().Where(x => x.Factory == expectedFactoryCode && x.WsNo.Contains(expectedFailCode)).FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetSampleAsync(expectedOkCode, expectedFactoryCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetSampleAsync(expectedFailCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetSampleFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedSample = GetExpectedSamples().Where(x => x.Factory == expectedFactoryCode && x.WsNo.Contains(expectedOkCode)).FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetSample(expectedOkCode, expectedFactoryCode))
                     .Returns(expectedSample!);
            var mockStream = new MemoryStream();
            _mockjson.Setup(service => service.Download(expectedSample!, $"sample_{expectedSample!.WsNo.ToLower()}.json"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetSampleFile(expectedOkCode, expectedFactoryCode, "json");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/json", fileResult.ContentType);
        }

        [TestMethod]
        public void GetSampleFile_ReturnsCsvSuccess()
        {
            // Arrange
            var expectedSample = GetExpectedSamples().Where(x => x.Factory == expectedFactoryCode && x.WsNo.Contains(expectedOkCode)).FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetSample(expectedOkCode, expectedFactoryCode))
                     .Returns(expectedSample!);
            var mockStream = new MemoryStream();
            _mockcsv.Setup(service => service.Download(expectedSample!, $"sample_{expectedSample!.WsNo.ToLower()}.csv"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetSampleFile(expectedOkCode, expectedFactoryCode, "csv");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("text/csv", fileResult.ContentType);
        }

        [TestMethod]
        public void GetSampleFile_ReturnsXmlSuccess()
        {
            // Arrange
            var expectedSample = GetExpectedSamples().Where(x => x.Factory == expectedFactoryCode && x.WsNo.Contains(expectedOkCode)).FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetSample(expectedOkCode, expectedFactoryCode))
                     .Returns(expectedSample!);
            var mockStream = new MemoryStream();
            _mockxml.Setup(service => service.Download(expectedSample!, $"sample_{expectedSample!.WsNo.ToLower()}.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetSampleFile(expectedOkCode, expectedFactoryCode, "xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }

        [TestMethod]
        public void SrhSamples_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples(); 
            _mockRepo.Setup(repo => repo.SearchSamples(expectedSrchCode))
                     .Returns(expectedSamples);

            // Act
            var result = _controller.SrhSamples(expectedSrchCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public void SrhSamples_ReturnsNotFound()
        {
            // Arrange;
            _mockRepo.Setup(repo => repo.SearchSamples(expectedFailCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.SrhSamples(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task SrhSamplesAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            _mockRepo.Setup(repo => repo.SearchSamplesAsync(expectedSrchCode))
                     .ReturnsAsync(expectedSamples);

            // Act
            var result = await _controller.SrhSamplesAsync(expectedSrchCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public async Task SrhSamplesAsync_ReturnsNotFound()
        {
            // Arrange;
            _mockRepo.Setup(repo => repo.SearchSamplesAsync(expectedFailCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.SrhSamplesAsync(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetSamplesByOp_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            _mockRepo.Setup(repo => repo.GetSamplesByOp(expectedOpCode))
                     .Returns(expectedSamples);

            // Act
            var result = _controller.GetSamplesByOp(expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public void GetSamplesByOp_ReturnsNotFound()
        {
            // Arrange;
            _mockRepo.Setup(repo => repo.GetSamplesByOp(expectedFailCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetSamplesByOp(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetSamplesByOpAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            _mockRepo.Setup(repo => repo.GetSamplesByOpAsync(expectedOpCode))
                     .ReturnsAsync(expectedSamples);

            // Act
            var result = await _controller.GetSamplesByOpAsync(expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public async Task GetSamplesByOpAsync_ReturnsNotFound()
        {
            // Arrange;
            _mockRepo.Setup(repo => repo.GetSamplesByOpAsync(expectedFailCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetSamplesByOpAsync(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetSamplesByOpStatus_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            _mockRepo.Setup(repo => repo.GetSamplesByOpStatus(expectedOpCode,expectedStatusCode))
                     .Returns(expectedSamples);

            // Act
            var result = _controller.GetSamplesByOpStatus(expectedOpCode, expectedStatusCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public void GetSamplesByOpStatus_ReturnsNotFound()
        {
            // Arrange;
            _mockRepo.Setup(repo => repo.GetSamplesByOpStatus(expectedFailCode, expectedFailCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetSamplesByOpStatus(expectedFailCode, expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetSamplesByOpStatusAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedSamples = GetExpectedSamples();
            _mockRepo.Setup(repo => repo.GetSamplesByOpStatusAsync(expectedOpCode, expectedStatusCode))
                     .ReturnsAsync(expectedSamples);

            // Act
            var result = await _controller.GetSamplesByOpStatusAsync(expectedOpCode, expectedStatusCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedSamples, okResult.Value);
        }

        [TestMethod]
        public async Task GetSamplesByOpStatusAsync_ReturnsNotFound()
        {
            // Arrange;
            _mockRepo.Setup(repo => repo.GetSamplesByOpStatusAsync(expectedFailCode, expectedFailCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetSamplesByOpStatusAsync(expectedFailCode, expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }
    }
}
