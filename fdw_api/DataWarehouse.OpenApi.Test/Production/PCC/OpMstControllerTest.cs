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
    public class OpMstControllerTest
    {
        private Mock<IOpMstRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<OpMst>> _mockcsv = null!;
        private Mock<IJsonResultService<OpMst>> _mockjson = null!;
        private Mock<IXmlResultService<OpMst>> _mockxml = null!;
        private OpMstController _controller = null!;

        private List<OpMst> GetExpectedOpMst()
        {
            return new List<OpMst>()
            {
                new OpMst() { Factory = "DS", OpCd = "UPC", Name = "Cutting" },
                new OpMst() { Factory = "DS", OpCd = "UPS", Name = "Stitching" }
            };
        }

        protected const string expectedOkCode = "UPS";
        protected const string expectedFailCode = "XXX";
        protected const string expectedFactory = "DS";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IOpMstRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockcsv = new Mock<ICsvResultService<OpMst>>();
            _mockjson = new Mock<IJsonResultService<OpMst>>();
            _mockxml = new Mock<IXmlResultService<OpMst>>();

            _controller = new OpMstController(_mockRepo.Object, _mockcsv.Object, _mockjson.Object, _mockxml.Object, _mockLogger.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger = null!;
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
            _mockjson.Setup(service => service.Download(expectedOpMst, "OpMst_list.json"))
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
            _mockcsv.Setup(service => service.Download(expectedOpMst, "OpMst_list.csv"))
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
            _mockxml.Setup(service => service.Download(expectedOpMst, "OpMst_list.xml"))
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
            var expectedOpMst = GetExpectedOpMst().Where(x=> x.Factory == expectedFactory && x.OpCd == expectedOkCode)
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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.OpCd == expectedFailCode)
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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.OpCd == expectedOkCode)
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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.OpCd == expectedFailCode)
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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.OpCd == expectedOkCode)
                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);
            _mockjson.Setup(service => service.Download(expectedOpMst!, "OpMst.json"))
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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.OpCd == expectedOkCode)
                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);
            _mockcsv.Setup(service => service.Download(expectedOpMst!, "OpMst.csv"))
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
            var expectedOpMst = GetExpectedOpMst().Where(x => x.Factory == expectedFactory && x.OpCd == expectedOkCode)
                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode, expectedFactory))
                     .Returns(expectedOpMst!);
            _mockxml.Setup(service => service.Download(expectedOpMst!, "OpMst.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode,expectedFactory,"xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }
    }
}