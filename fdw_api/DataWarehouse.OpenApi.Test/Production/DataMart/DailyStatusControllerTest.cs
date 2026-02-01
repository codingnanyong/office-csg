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
    public class DailyStatusControllerTest
    {
        #region Test Set-Up

        private Mock<IDailyStatusRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<DailyStatus>> _mockdailycsv = null!;
        private Mock<IJsonResultService<DailyStatus>> _mockdailyjson = null!;
        private Mock<IXmlResultService<DailyStatus>> _mockdailyxml = null!;
        private Mock<ICsvResultService<SampleWork>> _mockworkcsv = null!;
        private Mock<IJsonResultService<SampleWork>> _mockworkjson = null!;
        private Mock<IXmlResultService<SampleWork>> _mockworkxml = null!;
        private DailyStatusController _controller = null!;

        private List<DailyStatus> GetExpectedDailyStatus()
        {
            return new List<DailyStatus>()
            {
                new DailyStatus() { Factory ="DS" , OpCd = "UPC"},
                new DailyStatus() { Factory ="DS" , OpCd = "UPS"}
            };
        }
        private List<SampleWork> GetExpectedWorkList()
        {
            return new List<SampleWork>()
            {
                new SampleWork() { Factory ="DS" , OpCd = "UPC" },
                new SampleWork() { Factory ="DS" , OpCd = "UPS" }
            };
        }

        protected const string expectedFactoryCode = "DS";
        protected const string expectedOpCode = "UPC";
        protected const string expectedSrchCode = "zoom";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IDailyStatusRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockdailycsv = new Mock<ICsvResultService<DailyStatus>>();
            _mockdailyjson = new Mock<IJsonResultService<DailyStatus>>();
            _mockdailyxml = new Mock<IXmlResultService<DailyStatus>>();
            _mockworkcsv = new Mock<ICsvResultService<SampleWork>>();
            _mockworkjson = new Mock<IJsonResultService<SampleWork>>();
            _mockworkxml = new Mock<IXmlResultService<SampleWork>>();

            _controller = new DailyStatusController(_mockRepo.Object, _mockLogger.Object, _mockdailycsv.Object, _mockdailyjson.Object, _mockdailyxml.Object, _mockworkcsv.Object, _mockworkjson.Object, _mockworkxml.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger = null!;
            _mockdailycsv = null!;
            _mockdailyjson = null!;
            _mockdailyxml = null!;
            _mockworkcsv = null!;
            _mockworkjson = null!;
            _mockworkxml = null!;
            _controller = null!;
        }

        #endregion

        #region DailyStatus Test

        [TestMethod]
        public void GetAll_ReturnsOkResult()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus();
            _mockRepo.Setup(repo => repo.GetDailyStatus())
                     .Returns(expectedDailyStatus);

            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDailyStatus, okResult.Value);
        }

        [TestMethod]
        public void GetAll_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDailyStatus())
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
            var expectedDailyStatus = GetExpectedDailyStatus();
            _mockRepo.Setup(repo => repo.GetDailyStatusAsync())
                     .ReturnsAsync(expectedDailyStatus);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDailyStatus, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDailyStatusAsync())
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
            var expectedDailyStatus = GetExpectedDailyStatus();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatus())
                     .Returns(expectedDailyStatus);
            _mockdailyjson.Setup(service => service.Download(expectedDailyStatus, "dailystatus.json"))
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
            var expectedDailyStatus = GetExpectedDailyStatus();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatus())
                     .Returns(expectedDailyStatus);
            _mockdailycsv.Setup(service => service.Download(expectedDailyStatus, "dailystatus.csv"))
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
            var expectedDailyStatus = GetExpectedDailyStatus();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatus())
                     .Returns(expectedDailyStatus);
            _mockdailyxml.Setup(service => service.Download(expectedDailyStatus, "dailystatus.xml"))
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
        public void GetByFactory_ReturnsOkResult()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x=> x.Factory == expectedFactoryCode)
                                                              .ToList();
            _mockRepo.Setup(repo => repo.GetDailyStatusByFactory(expectedFactoryCode))
                     .Returns(expectedDailyStatus);

            // Act
            var result = _controller.GetByFactory(expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDailyStatus, okResult.Value);
        }

        [TestMethod]
        public void GetByFactory_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDailyStatusByFactory(expectedFactoryCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetByFactory(expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetByFactoryAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode)
                                                              .ToList();
            _mockRepo.Setup(repo => repo.GetDailyStatusByFactoryAsync(expectedFactoryCode))
                     .ReturnsAsync(expectedDailyStatus);

            // Act
            var result = await _controller.GetByFactoryAsync(expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDailyStatus, okResult.Value);
        }

        [TestMethod]
        public async Task GetByFactoryAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDailyStatusByFactoryAsync(expectedFactoryCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetByFactoryAsync(expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetByFactoryFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode)
                                                              .ToList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatusByFactory(expectedFactoryCode))
                     .Returns(expectedDailyStatus);
            _mockdailyjson.Setup(service => service.Download(expectedDailyStatus, $"{expectedFactoryCode.ToLower()}_dailystatus.json"))
                          .Returns(mockStream);

            // Act
            var result = _controller.GetByFactoryFile(expectedFactoryCode,"json");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/json", fileResult.ContentType);
        }

        [TestMethod]
        public void GetByFactoryFile_ReturnsCsvSuccess()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode)
                                                             .ToList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatusByFactory(expectedFactoryCode))
                     .Returns(expectedDailyStatus);
            _mockdailycsv.Setup(service => service.Download(expectedDailyStatus, $"{expectedFactoryCode.ToLower()}_dailystatus.csv"))
                         .Returns(mockStream);

            // Act
            var result = _controller.GetByFactoryFile(expectedFactoryCode,"csv");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("text/csv", fileResult.ContentType);
        }

        [TestMethod]
        public void GetByFactoryFile_ReturnsXmlSuccess()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode)
                                                              .ToList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatusByFactory(expectedFactoryCode))
                     .Returns(expectedDailyStatus);
            _mockdailyxml.Setup(service => service.Download(expectedDailyStatus, $"{expectedFactoryCode.ToLower()}_dailystatus.xml"))
                         .Returns(mockStream);

            // Act
            var result = _controller.GetByFactoryFile(expectedFactoryCode, "xml");

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
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode)
                                                              .FirstOrDefault();
            _mockRepo.Setup(repo => repo.GetDailyStatus(expectedOpCode, expectedFactoryCode))
                     .Returns(expectedDailyStatus!);

            // Act
            var result = _controller.Get(expectedOpCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDailyStatus, okResult.Value);
        }

        [TestMethod]
        public void Get_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDailyStatus(expectedOpCode, expectedFactoryCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.Get(expectedFactoryCode,expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode)
                                                              .FirstOrDefault() ?? null!;
            _mockRepo.Setup(repo => repo.GetDailyStatusAsync(expectedOpCode, expectedFactoryCode))
                     .ReturnsAsync(expectedDailyStatus);

            // Act
            var result = await _controller.GetAsync(expectedOpCode,expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedDailyStatus, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetDailyStatusAsync(expectedOpCode, expectedFactoryCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetAsync(expectedFactoryCode, expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode)
                                                              .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatus(expectedOpCode,expectedFactoryCode))
                     .Returns(expectedDailyStatus!);
            _mockdailyjson.Setup(service => service.Download(expectedDailyStatus!, $"{expectedFactoryCode.ToLower()}_{expectedOpCode.ToLower()}_dailystatus.json"))
                          .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOpCode, expectedFactoryCode, "json");

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
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode)
                                                              .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatus(expectedOpCode, expectedFactoryCode))
                     .Returns(expectedDailyStatus!);
            _mockdailycsv.Setup(service => service.Download(expectedDailyStatus!, $"{expectedFactoryCode.ToLower()}_{expectedOpCode.ToLower()}_dailystatus.csv"))
                         .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOpCode, expectedFactoryCode, "csv");

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
            var expectedDailyStatus = GetExpectedDailyStatus().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode)
                                                              .FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetDailyStatus(expectedOpCode, expectedFactoryCode))
                     .Returns(expectedDailyStatus!);
            _mockdailyxml.Setup(service => service.Download(expectedDailyStatus!, $"{expectedFactoryCode.ToLower()}_{expectedOpCode.ToLower()}_dailystatus.xml"))
                         .Returns(mockStream);

            // Act
            var result = _controller.GetFile(expectedOpCode, expectedFactoryCode, "xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }

        #endregion

        #region DailyStatus WorkList Test

        [TestMethod]
        public void GetWorkList_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            _mockRepo.Setup(repo => repo.GetWorkList())
                     .Returns(expectedWorkList);

            // Act
            var result = _controller.GetWorkList();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public void GetWorkList_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetWorkList())
                     .Returns(() => null!);
            // Act
            var result = _controller.GetWorkList();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetWorkListAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            _mockRepo.Setup(repo => repo.GetWorkListAsync())
                     .ReturnsAsync(expectedWorkList);

            // Act
            var result = await _controller.GetWorkListAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public async Task GetWorkListAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetWorkListAsync())
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetWorkListAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetWorkListFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetWorkList())
                     .Returns(expectedWorkList);
            _mockworkjson.Setup(service => service.Download(expectedWorkList, "dailystatus_worklist.json"))
                         .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListFile("json");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/json", fileResult.ContentType);
        }

        [TestMethod]
        public void GetWorkListFile_ReturnsCsvSuccess()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetWorkList())
                     .Returns(expectedWorkList);
            _mockworkcsv.Setup(service => service.Download(expectedWorkList, "dailystatus_worklist.csv"))
                        .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListFile("csv");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("text/csv", fileResult.ContentType);
        }

        [TestMethod]
        public void GetWorkListFile_ReturnsXmlSuccess()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetWorkList())
                     .Returns(expectedWorkList);
            _mockworkxml.Setup(service => service.Download(expectedWorkList, "dailystatus_worklist.xml"))
                        .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListFile("xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }

        [TestMethod]
        public void GetWorkListBy_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).ToList();
            _mockRepo.Setup(repo => repo.GetWorkList(expectedOpCode,expectedFactoryCode))
                     .Returns(expectedWorkList);

            // Act
            var result = _controller.GetWorkListBy(expectedOpCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public void GetWorkListBy_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetWorkListBy(expectedOpCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetWorkListByAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).ToList();
            _mockRepo.Setup(repo => repo.GetWorkListAsync(expectedOpCode, expectedFactoryCode))
                     .ReturnsAsync(expectedWorkList);

            // Act
            var result = await _controller.GetWorkListByAsync(expectedOpCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public async Task GetWorkListByAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetWorkListAsync(expectedOpCode, expectedFactoryCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetWorkListByAsync(expectedOpCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetWorkListByFile_ReturnsJsonSuccess()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).ToList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
                     .Returns(expectedWorkList);
            _mockworkjson.Setup(service => service.Download(expectedWorkList, $"dailystatus_worklist{expectedOpCode.ToLower()}_list.json"))
                         .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListByFile(expectedOpCode, expectedFactoryCode,"json");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/json", fileResult.ContentType);
        }

        [TestMethod]
        public void GetWorkListByFile_ReturnsCsvSuccess()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).ToList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
                     .Returns(expectedWorkList);
            _mockworkcsv.Setup(service => service.Download(expectedWorkList, $"dailystatus_worklist{expectedOpCode.ToLower()}_list.csv"))
                        .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListByFile(expectedOpCode, expectedFactoryCode,"csv");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("text/csv", fileResult.ContentType);
        }

        [TestMethod]
        public void GetWorkListByFile_ReturnsXmlSuccess()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).ToList();
            var mockStream = new MemoryStream();
            _mockRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
                     .Returns(expectedWorkList);
            _mockworkxml.Setup(service => service.Download(expectedWorkList, $"dailystatus_worklist{expectedOpCode.ToLower()}_list.xml"))
                        .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListByFile(expectedOpCode, expectedFactoryCode,"xml");

            // Assert
            Assert.IsInstanceOfType(result, typeof(FileResult));
            var fileResult = result as FileResult;
            Assert.IsNotNull(fileResult);
            Assert.AreEqual("application/xml", fileResult.ContentType);
        }

        [TestMethod]
        public void GetSearch_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode ).ToList();
            _mockRepo.Setup(repo => repo.GetSearch(expectedOpCode,expectedSrchCode, expectedFactoryCode))
                     .Returns(expectedWorkList);

            // Act
            var result = _controller.GetSearch(expectedOpCode, expectedSrchCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public void GetSearch_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetSearch(expectedOpCode, expectedSrchCode, expectedFactoryCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetSearch(expectedOpCode, expectedSrchCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetSearchAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).ToList();
            _mockRepo.Setup(repo => repo.GetSearchAsync(expectedOpCode, expectedSrchCode, expectedFactoryCode))
                     .ReturnsAsync(expectedWorkList);

            // Act
            var result = await _controller.GetSearchAsync(expectedOpCode, expectedSrchCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public async Task GetSearchAsync_NotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetSearchAsync(expectedOpCode, expectedSrchCode, expectedFactoryCode))
                     .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetSearchAsync(expectedOpCode, expectedSrchCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion
    }
}
