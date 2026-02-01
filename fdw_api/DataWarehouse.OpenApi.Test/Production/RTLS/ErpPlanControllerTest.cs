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
    public class ErpPlanControllerTest
    {
        private Mock<IErpPlanRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<ErpPlan>> _mockcsv = null!;
        private Mock<IJsonResultService<ErpPlan>> _mockjson = null!;
        private Mock<IXmlResultService<ErpPlan>> _mockxml = null!;
        private ErpPlanController _controller = null!;

        private List<ErpPlan> GetExpectedErpPlan()
        {
            return new List<ErpPlan>()
            {
                new ErpPlan() { Factory = "DS" ,BeaconTagId = "fb1226bbb001"},
                new ErpPlan() { Factory = "DS" ,BeaconTagId = "fb1226bbb003"}
            };
        }

        protected const string expectedOkCode = "fb1226bbb001";
        protected const string expectedFailCode = "XXX";
        protected const string expectedFactory = "DS";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IErpPlanRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockcsv = new Mock<ICsvResultService<ErpPlan>>();
            _mockjson = new Mock<IJsonResultService<ErpPlan>>();
            _mockxml = new Mock<IXmlResultService<ErpPlan>>();
            _controller = new ErpPlanController(_mockRepo.Object, _mockcsv.Object, _mockjson.Object, _mockxml.Object, _mockLogger.Object);
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
            var expectedMachineMst = GetExpectedErpPlan();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
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
            var expectedMachineMst = GetExpectedErpPlan();
            _mockRepo.Setup(repo => repo.GetCurrentAllAsync())
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
            var expectedErpPlan = GetExpectedErpPlan();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
                     .Returns(expectedErpPlan);
            _mockjson.Setup(service => service.Download(expectedErpPlan, "cur_plan_list.json"))
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
            var expectedErpPlan = GetExpectedErpPlan();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
                     .Returns(expectedErpPlan);
            _mockcsv.Setup(service => service.Download(expectedErpPlan, "cur_plan_list.csv"))
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
            var expectedErpPlan = GetExpectedErpPlan();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrentAll())
                     .Returns(expectedErpPlan);
            _mockxml.Setup(service => service.Download(expectedErpPlan, "cur_plan_list.xml"))
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
            var expectedErpPlan = GetExpectedErpPlan().Where(x => x.Factory == expectedFactory && x.BeaconTagId == expectedOkCode)
                                                      .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedErpPlan!);

            // Act
            var result = _controller.Get(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedErpPlan, okResult.Value);
        }

        [TestMethod]
        public void Get_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedErpPlan = GetExpectedErpPlan().Where(x => x.Factory == expectedFactory && x.BeaconTagId == expectedFailCode)
                                                      .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedErpPlan!);

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
            var expectedErpPlan = GetExpectedErpPlan().Where(x => x.Factory == expectedFactory && x.BeaconTagId == expectedOkCode)
                                                      .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrentAsync(expectedOkCode))
                     .ReturnsAsync(expectedErpPlan!);

            // Act
            var result = await _controller.GetAsync(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedErpPlan, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_ReturnsNotFoundResult()
        {
            // Arrange
            var expectedErpPlan = GetExpectedErpPlan().Where(x => x.Factory == expectedFactory && x.BeaconTagId == expectedFailCode)
                                                      .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetCurrentAsync(expectedOkCode))
                     .ReturnsAsync(expectedErpPlan!);

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
            var expectedErpPlan = GetExpectedErpPlan().Where(x => x.Factory == expectedFactory && x.BeaconTagId == expectedOkCode)
                                                      .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedErpPlan!);
            _mockjson.Setup(service => service.Download(expectedErpPlan!, "slc_cur_plan.json"))
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
            var expectedErpPlan = GetExpectedErpPlan().Where(x => x.Factory == expectedFactory && x.BeaconTagId == expectedOkCode)
                                                      .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedErpPlan!);
            _mockcsv.Setup(service => service.Download(expectedErpPlan!, "slc_cur_plan.csv"))
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
            var expectedErpPlan = GetExpectedErpPlan().Where(x => x.Factory == expectedFactory && x.BeaconTagId == expectedOkCode)
                                                      .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetCurrent(expectedOkCode))
                     .Returns(expectedErpPlan!);
            _mockxml.Setup(service => service.Download(expectedErpPlan!, "slc_cur_plan.xml"))
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
