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
    public class ProcessLocMstControllerTest
    {
        private Mock<IProcessMstRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<ProcessMst>> _mockcsv = null!;
        private Mock<IJsonResultService<ProcessMst>> _mockjson = null!;
        private Mock<IXmlResultService<ProcessMst>> _mockxml = null!;
        private ProcessLocMstController _controller = null!;

        private List<ProcessMst> GetExpectedProcessLocMst()
        {
            return new List<ProcessMst>()
            {
                new ProcessMst() { ProcessLoc = "s001", ProcessLocName = "재봉1", LocName = "재봉투입" ,Level1 = "UP" , Level2 = "UPS"},
                new ProcessMst() { ProcessLoc = "s002", ProcessLocName = "재봉2", LocName = "재봉투입" ,Level1 = "UP" , Level2 = "UPS"},
                new ProcessMst() { ProcessLoc = "cs15", ProcessLocName = "컴재봉12", LocName = "컴재봉투입" ,Level1 = "UP" , Level2 = "UPS"}
            };
        }

        protected const string expectedOkCode = "s001";
        protected const string expectedFailCode = "XXX";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IProcessMstRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockcsv = new Mock<ICsvResultService<ProcessMst>>();
            _mockjson = new Mock<IJsonResultService<ProcessMst>>();
            _mockxml = new Mock<IXmlResultService<ProcessMst>>();

            _controller = new ProcessLocMstController(_mockRepo.Object, _mockcsv.Object, _mockjson.Object, _mockxml.Object, _mockLogger.Object);
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
            var expectedProcessLocMst = GetExpectedProcessLocMst();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedProcessLocMst);

            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedProcessLocMst, okResult.Value);
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
            var expectedProcessLocMst = GetExpectedProcessLocMst();
            _mockRepo.Setup(repo => repo.GetAllAsync())
                     .ReturnsAsync(expectedProcessLocMst);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedProcessLocMst, okResult.Value);
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
            var expectedProcessLocMst = GetExpectedProcessLocMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedProcessLocMst);
            _mockjson.Setup(service => service.Download(expectedProcessLocMst, "processMst_list.json"))
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
            var expectedProcessLocMst = GetExpectedProcessLocMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedProcessLocMst);
            _mockcsv.Setup(service => service.Download(expectedProcessLocMst, "processMst_list.csv"))
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
            var expectedProcessLocMst = GetExpectedProcessLocMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedProcessLocMst);
            _mockxml.Setup(service => service.Download(expectedProcessLocMst, "processMst_list.xml"))
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
            var expectedProcessLocMst = GetExpectedProcessLocMst().Where(x => x.ProcessLoc == expectedOkCode)
                                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode))
                     .Returns(expectedProcessLocMst!);

            // Act
            var result = _controller.Get(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedProcessLocMst, okResult.Value);
        }

        [TestMethod]
        public void Get_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedProcessLocMst = GetExpectedProcessLocMst().Where(x => x.ProcessLoc == expectedFailCode)
                                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode))
                     .Returns(expectedProcessLocMst!);

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
            var expectedProcessLocMst = GetExpectedProcessLocMst().Where(x => x.ProcessLoc == expectedOkCode)
                                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetAsync(expectedOkCode))
                     .ReturnsAsync(expectedProcessLocMst!);

            // Act
            var result = await _controller.GetAsync(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedProcessLocMst, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedProcessLocMst = GetExpectedProcessLocMst().Where(x => x.ProcessLoc == expectedFailCode)
                                                                  .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetAsync(expectedOkCode))
                     .ReturnsAsync(expectedProcessLocMst!);

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
            var expectedProcessLocMst = GetExpectedProcessLocMst().Where(x => x.ProcessLoc == expectedOkCode)
                                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode))
                     .Returns(expectedProcessLocMst!);
            _mockjson.Setup(service => service.Download(expectedProcessLocMst!, "processMst.json"))
                     .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode , "json");

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
            var expectedProcessLocMst = GetExpectedProcessLocMst().Where(x => x.ProcessLoc == expectedOkCode)
                                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode))
                     .Returns(expectedProcessLocMst!);
            _mockcsv.Setup(service => service.Download(expectedProcessLocMst!, "processMst.csv"))
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
            var expectedProcessLocMst = GetExpectedProcessLocMst().Where(x => x.ProcessLoc == expectedOkCode)
                                                                  .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode))
                     .Returns(expectedProcessLocMst!);
            _mockxml.Setup(service => service.Download(expectedProcessLocMst!, "processMst.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOkCode , "xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }
    }
}
