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
    public class MachineMstTest
    {
        private Mock<IMachineMstRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<MachineMst>> _mockcsv = null!;
        private Mock<IJsonResultService<MachineMst>> _mockjson = null!;
        private Mock<IXmlResultService<MachineMst>> _mockxml = null!;
        private MachineMstController _controller = null!;

        private List<MachineMst> GetExpectedMachineMst()
        {
            return new List<MachineMst>()
            {
                new MachineMst() { }
            };
        }

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IMachineMstRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockcsv = new Mock<ICsvResultService<MachineMst>>();
            _mockjson = new Mock<IJsonResultService<MachineMst>>();
            _mockxml = new Mock<IXmlResultService<MachineMst>>();

            _controller = new MachineMstController(_mockRepo.Object, _mockcsv.Object, _mockjson.Object, _mockxml.Object, _mockLogger.Object);
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
            var expectedMachineMst = GetExpectedMachineMst();
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
        public async Task GetAllAsync_OkResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedMachineMst();
            _mockRepo.Setup(repo => repo.GetAllAsync())
                     .ReturnsAsync(expectedEnviromentData);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedEnviromentData, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_NotFoundResult()
        {
            // Arrange
            var expectedEnviromentData = GetExpectedMachineMst();
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
            var expectedOpMst = GetExpectedMachineMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedOpMst);
            _mockjson.Setup(service => service.Download(expectedOpMst, "Machine_list.json"))
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
            var expectedOpMst = GetExpectedMachineMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedOpMst);
            _mockcsv.Setup(service => service.Download(expectedOpMst, "Machine_list.csv"))
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
            var expectedOpMst = GetExpectedMachineMst();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetAll())
                     .Returns(expectedOpMst);
            _mockxml.Setup(service => service.Download(expectedOpMst, "Machine_list.xml"))
                    .Returns(mockStream);

            // Act
            var result = _controller.GetAllFile("xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }
    }
}
