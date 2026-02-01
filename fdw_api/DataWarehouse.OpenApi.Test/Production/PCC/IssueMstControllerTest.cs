using CSG.MI.DAO.Production.PCC;
using CSG.MI.FDW.BLL.Production.PCC.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using CSG.MI.FDW.OpenApi.Infrastructure.Formatter;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Production.PCC
{
    [TestClass]
    public class IssueMstControllerTest
    {
        private Mock<IIssueMstRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<IssueMst>> _mockcsv = null!;
        private Mock<IJsonResultService<IssueMst>> _mockjson = null!;
        private Mock<IXmlResultService<IssueMst>> _mockxml = null!;
        private IssueMstController _controller = null!;

        private List<IssueMst> GetExpectedOpMst()
        {
            return new List<IssueMst>()
            {
                new IssueMst() { Factory = "DS", IssueCd = "A0000", IssueName = "자재" },
                new IssueMst() { Factory = "DS", IssueCd = "B0000", IssueName = "패턴" }
            };
        }

        protected const string expectedFactory = "DS";
        protected const string expectedOkCode = "A0000";
        protected const string expectedFailCode = "XXX";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IIssueMstRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockcsv = new Mock<ICsvResultService<IssueMst>>();
            _mockjson = new Mock<IJsonResultService<IssueMst>>();
            _mockxml = new Mock<IXmlResultService<IssueMst>>();

            _controller = new IssueMstController(_mockRepo.Object, _mockcsv.Object, _mockjson.Object, _mockxml.Object, _mockLogger.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger= null!;
            _mockjson = null!;
            _mockcsv = null!;
            _mockxml = null!;
            _controller = null!;
        }

        [TestMethod]
        public void GetAll_ReturnsOkResult()
        {
            // Arrange
            var expectedMachineMst = GetExpectedOpMst();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedMachineMst);
            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedMachineMst, okResult.Value);
        }

        [TestMethod]
        public void GetAll_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetAll())
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
            var expectedMachineMst = GetExpectedOpMst();
            _mockRepo.Setup(repo => repo.GetAllAsync())
                     .ReturnsAsync(expectedMachineMst);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedMachineMst, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetAllAsync())
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
            var expectedOpMst = GetExpectedOpMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedOpMst);
            _mockjson.Setup(service => service.Download(expectedOpMst, "IssuMst_List.json"))
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
            var expectedOpMst = GetExpectedOpMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedOpMst);
            _mockcsv.Setup(service => service.Download(expectedOpMst, "IssuMst_List.csv"))
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
            var expectedOpMst = GetExpectedOpMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedOpMst);
            _mockxml.Setup(service => service.Download(expectedOpMst, "IssuMst_List.xml"))
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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.IssueCd == expectedOkCode)
                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);

            // Act
            var result = _controller.Get(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedOpMst, okResult.Value);
        }

        [TestMethod]
        public void Get_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.IssueCd == expectedFailCode)
                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);

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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.IssueCd == expectedOkCode)
                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetAsync(expectedOkCode, expectedFactory))
                     .ReturnsAsync(expectedOpMst!);

            // Act
            var result = await _controller.GetAsync(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedOpMst, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.IssueCd == expectedFailCode)
                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetAsync(expectedOkCode, expectedFactory))
                     .ReturnsAsync(expectedOpMst!);

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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.IssueCd == expectedOkCode)
                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);
            _mockjson.Setup(service => service.Download(expectedOpMst!, "IssuMst.json"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode, expectedFactory, "json");

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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.IssueCd == expectedOkCode)
                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);
            _mockcsv.Setup(service => service.Download(expectedOpMst!, "IssuMst.csv"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode, expectedFactory, "csv");

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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.IssueCd == expectedOkCode)
                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);
            _mockxml.Setup(service => service.Download(expectedOpMst!, "IssuMst.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode, expectedFactory, "xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }
    }
}
