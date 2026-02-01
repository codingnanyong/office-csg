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
    public class WipControllerTest
    {
        #region Test Set-Up

        private Mock<IWsWipRepo> _mockwipRepo = null!;
        private Mock<IWsWipRateRepo> _mockwiprateRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private Mock<ICsvResultService<Wip>> _mockwipcsv = null!;
        private Mock<IJsonResultService<Wip>> _mockwipjson = null!;
        private Mock<IXmlResultService<Wip>> _mockwipxml = null!;
        private Mock<ICsvResultService<SampleWork>> _mockworkcsv = null!;
        private Mock<IJsonResultService<SampleWork>> _mockworkjson = null!;
        private Mock<IXmlResultService<SampleWork>> _mockworkxml = null!;
        private WipController _controller = null!;

        private List<Wip> GetExpectedWip()
        {
            return new List<Wip>()
            {
                new Wip() { Factory ="DS" , OpCd = "UPC"},
                new Wip() { Factory ="DS" , OpCd = "UPS"}
            };
        }
        private List<WipRate> GetExpectedWipRate()
        {
            return new List<WipRate>()
            {
                new WipRate() { OpCd = "UPC" , OpName = "Cutting" },
                new WipRate() { OpCd = "UPS" , OpName = "Stitching" }
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
        protected const string expectedStatusCode = "I";
        protected const string expectedSrchCode = "zoom";

        [TestInitialize]
        public void Setup()
        {
            _mockwipRepo = new Mock<IWsWipRepo>();
            _mockwiprateRepo = new Mock<IWsWipRateRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _mockwipcsv = new Mock<ICsvResultService<Wip>>();
            _mockwipjson = new Mock<IJsonResultService<Wip>>();
            _mockwipxml = new Mock<IXmlResultService<Wip>>();
            _mockworkcsv = new Mock<ICsvResultService<SampleWork>>();
            _mockworkjson = new Mock<IJsonResultService<SampleWork>>();
            _mockworkxml = new Mock<IXmlResultService<SampleWork>>();

            _controller = new WipController(_mockwipRepo.Object, _mockwiprateRepo.Object, _mockLogger.Object, _mockwipcsv.Object, _mockwipjson.Object, _mockwipxml.Object, _mockworkcsv.Object, _mockworkjson.Object, _mockworkxml.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockwipRepo = null!;
            _mockLogger = null!;
            _mockwipcsv = null!;
            _mockwipjson = null!;
            _mockwipxml = null!;
            _mockworkcsv = null!;
            _mockworkjson = null!;
            _mockworkxml = null!;
            _controller = null!;
        }

        #endregion

        #region Wip Test

        [TestMethod]
        public void GetAll_ReturnsOkResult()
        {
            // Arrange
            var expectedWip = GetExpectedWip();
            _mockwipRepo.Setup(repo => repo.GetAll())
                        .Returns(expectedWip);

            // Act
            var result = _controller.GetAll();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWip, okResult.Value);
        }

        [TestMethod]
        public void GetAll_NotFoundResult()
        {
            // Arrange
            _mockwipRepo.Setup(repo => repo.GetAll())
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
            var expectedWip = GetExpectedWip();
            _mockwipRepo.Setup(repo => repo.GetAllAsync())
                        .ReturnsAsync(expectedWip);

            // Act
            var result = await _controller.GetAllAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWip, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_NotFoundResult()
        {
            // Arrange
            _mockwipRepo.Setup(repo => repo.GetAllAsync())
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
            var expectedWip = GetExpectedWip();
            _mockwipRepo.Setup(repo => repo.GetAll())
                        .Returns(expectedWip);
            var mockStream = new MemoryStream();
            _mockwipjson.Setup(service => service.Download(expectedWip, "wip_list.json"))
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
            var expectedWip = GetExpectedWip();
            _mockwipRepo.Setup(repo => repo.GetAll())
                        .Returns(expectedWip);
            var mockStream = new MemoryStream();
            _mockwipcsv.Setup(service => service.Download(expectedWip, "wip_list.csv"))
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
            var expectedWip = GetExpectedWip();
            _mockwipRepo.Setup(repo => repo.GetAll())
                        .Returns(expectedWip);
            var mockStream = new MemoryStream();
            _mockwipxml.Setup(service => service.Download(expectedWip, "wip_list.xml"))
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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode).ToList();
            _mockwipRepo.Setup(repo => repo.GetByFactory(expectedFactoryCode))
                        .Returns(expectedWip);

            // Act
            var result = _controller.GetByFactory(expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWip, okResult.Value);
        }

        [TestMethod]
        public void GetByFactory_NotFoundResult()
        {
            // Arrange
            _mockwipRepo.Setup(repo => repo.GetByFactory(expectedFactoryCode))
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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode).ToList();
            _mockwipRepo.Setup(repo => repo.GetByFactoryAsync(expectedFactoryCode))
                        .ReturnsAsync(expectedWip);

            // Act
            var result = await _controller.GetByFactoryAsync(expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWip, okResult.Value);
        }

        [TestMethod]
        public async Task GetByFactoryAsync_NotFoundResult()
        {
            // Arrange
            _mockwipRepo.Setup(repo => repo.GetByFactoryAsync(expectedFactoryCode))
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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode).ToList();
            var mockStream = new MemoryStream();

            _mockwipRepo.Setup(repo => repo.GetByFactory(expectedFactoryCode))
                        .Returns(expectedWip);
            _mockwipjson.Setup(service => service.Download(expectedWip, $"wip_list_{expectedFactoryCode.ToLower()}.json"))
                        .Returns(mockStream);

            // Act
            var result = _controller.GetByFactoryFile(expectedFactoryCode, "json");

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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode).ToList();
            var mockStream = new MemoryStream();

            _mockwipRepo.Setup(repo => repo.GetByFactory(expectedFactoryCode))
                        .Returns(expectedWip);
            _mockwipcsv.Setup(service => service.Download(expectedWip, $"wip_list_{expectedFactoryCode.ToLower()}.csv"))
                       .Returns(mockStream);

            // Act
            var result = _controller.GetByFactoryFile(expectedFactoryCode, "csv");

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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode).ToList();
            var mockStream = new MemoryStream();

            _mockwipRepo.Setup(repo => repo.GetByFactory(expectedFactoryCode))
                        .Returns(expectedWip);
            _mockwipxml.Setup(service => service.Download(expectedWip, $"wip_list_{expectedFactoryCode.ToLower()}.xml"))
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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).FirstOrDefault();
            _mockwipRepo.Setup(repo => repo.GetWip(expectedOpCode, expectedFactoryCode))
                     .Returns(expectedWip!);

            // Act
            var result = _controller.Get(expectedOpCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWip, okResult.Value);
        }

        [TestMethod]
        public void Get_NotFoundResult()
        {
            // Arrange
            _mockwipRepo.Setup(repo => repo.GetWip(expectedOpCode, expectedFactoryCode))
                        .Returns(() => null!);

            // Act
            var result = _controller.Get(expectedFactoryCode, expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).FirstOrDefault() ?? null!;
            _mockwipRepo.Setup(repo => repo.GetWipAsync(expectedOpCode, expectedFactoryCode))
                        .ReturnsAsync(expectedWip);

            // Act
            var result = await _controller.GetAsync(expectedOpCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWip, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_NotFoundResult()
        {
            // Arrange
            _mockwipRepo.Setup(repo => repo.GetWipAsync(expectedOpCode, expectedFactoryCode))
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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockwipRepo.Setup(repo => repo.GetWip(expectedOpCode, expectedFactoryCode))
                        .Returns(expectedWip!);
            _mockwipjson.Setup(service => service.Download(expectedWip!, $"{expectedOpCode.ToLower()}_wip.json"))
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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockwipRepo.Setup(repo => repo.GetWip(expectedOpCode, expectedFactoryCode))
                        .Returns(expectedWip!);
            _mockwipcsv.Setup(service => service.Download(expectedWip!, $"{expectedOpCode.ToLower()}_wip.csv"))
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
            var expectedWip = GetExpectedWip().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).FirstOrDefault();
            var mockStream = new MemoryStream();
            _mockwipRepo.Setup(repo => repo.GetWip(expectedOpCode, expectedFactoryCode))
                        .Returns(expectedWip!);
            _mockwipxml.Setup(service => service.Download(expectedWip!, $"{expectedOpCode.ToLower()}_wip.xml"))
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

        #region Wip WorkList Test

        [TestMethod]
        public void GetWorkList_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            _mockwipRepo.Setup(repo => repo.GetWorkList())
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
            _mockwipRepo.Setup(repo => repo.GetWorkList())
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
            _mockwipRepo.Setup(repo => repo.GetWorkListAsync())
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
            _mockwipRepo.Setup(repo => repo.GetWorkListAsync())
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
            _mockwipRepo.Setup(repo => repo.GetWorkList())
                        .Returns(expectedWorkList);
            _mockworkjson.Setup(service => service.Download(expectedWorkList, "wip_worklist.json"))
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
            _mockwipRepo.Setup(repo => repo.GetWorkList())
                        .Returns(expectedWorkList);
            _mockworkcsv.Setup(service => service.Download(expectedWorkList, "wip_worklist.csv"))
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
            _mockwipRepo.Setup(repo => repo.GetWorkList())
                        .Returns(expectedWorkList);
            _mockworkxml.Setup(service => service.Download(expectedWorkList, "wip_worklist.xml"))
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
            _mockwipRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
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
            _mockwipRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
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
            _mockwipRepo.Setup(repo => repo.GetWorkListAsync(expectedOpCode, expectedFactoryCode))
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
            _mockwipRepo.Setup(repo => repo.GetWorkListAsync(expectedOpCode, expectedFactoryCode))
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
            _mockwipRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
                        .Returns(expectedWorkList);
            _mockworkjson.Setup(service => service.Download(expectedWorkList, $"wip_worklist_{expectedFactoryCode.ToLower()}_{expectedOpCode.ToLower()}.json"))
                         .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListByFile(expectedOpCode, expectedFactoryCode, "json");

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
            _mockwipRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
                        .Returns(expectedWorkList);
            _mockworkcsv.Setup(service => service.Download(expectedWorkList, $"wip_worklist_{expectedFactoryCode.ToLower()}_{expectedOpCode.ToLower()}.csv"))
                        .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListByFile(expectedOpCode, expectedFactoryCode, "csv");

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
            _mockwipRepo.Setup(repo => repo.GetWorkList(expectedOpCode, expectedFactoryCode))
                        .Returns(expectedWorkList);
            _mockworkxml.Setup(service => service.Download(expectedWorkList, $"wip_worklist_{expectedFactoryCode.ToLower()}_{expectedOpCode.ToLower()}.xml"))
                        .Returns(mockStream);

            // Act
            var result = _controller.GetWorkListByFile(expectedOpCode, expectedFactoryCode, "xml");

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
            var expectedWorkList = GetExpectedWorkList().Where(x => x.Factory == expectedFactoryCode && x.OpCd == expectedOpCode).ToList();
            _mockwipRepo.Setup(repo => repo.GetSearch(expectedOpCode, expectedSrchCode, expectedFactoryCode))
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
            _mockwipRepo.Setup(repo => repo.GetSearch(expectedOpCode, expectedSrchCode, expectedFactoryCode))
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
            _mockwipRepo.Setup(repo => repo.GetSearchAsync(expectedOpCode, expectedSrchCode, expectedFactoryCode))
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
            _mockwipRepo.Setup(repo => repo.GetSearchAsync(expectedOpCode, expectedSrchCode, expectedFactoryCode))
                        .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetSearchAsync(expectedOpCode, expectedSrchCode, expectedFactoryCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion

        #region Wip Rate Test

        [TestMethod]
        public void GetRates_ReturnsOkResult()
        {
            // Arrange
            var expectedWipRate = GetExpectedWipRate();
            _mockwiprateRepo.Setup(repo => repo.GetWipRates())
                            .Returns(expectedWipRate);

            // Act
            var result = _controller.GetRates();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWipRate, okResult.Value);
        }

        [TestMethod]
        public void GetRates_NotFoundResult()
        {
            // Arrange
            _mockwiprateRepo.Setup(repo => repo.GetWipRates())
                            .Returns(() => null!);

            // Act
            var result = _controller.GetRates();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetRatesAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedWipRate = GetExpectedWipRate();
            _mockwiprateRepo.Setup(repo => repo.GetWipRatesAsync())
                            .ReturnsAsync(expectedWipRate);

            // Act
            var result = await _controller.GetRatesAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWipRate, okResult.Value);
        }

        [TestMethod]
        public async Task GetRatesAsync_NotFoundResult()
        {
            // Arrange
            _mockwiprateRepo.Setup(repo => repo.GetWipRatesAsync())
                            .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetRatesAsync();

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetRate_ReturnsOkResult()
        {
            // Arrange
            var expectedWipRate = GetExpectedWipRate().Where(x => x.OpCd == expectedOpCode).FirstOrDefault();
            _mockwiprateRepo.Setup(repo => repo.GetWipRate(expectedOpCode))
                            .Returns(expectedWipRate!);

            // Act
            var result = _controller.GetRate(expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWipRate, okResult.Value);
        }

        [TestMethod]
        public void GetRate_NotFoundResult()
        {
            // Arrange
            _mockwiprateRepo.Setup(repo => repo.GetWipRate(expectedOpCode))
                            .Returns(() => null!);

            // Act
            var result = _controller.GetRate(expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetRateAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedWipRate = GetExpectedWipRate().Where(x => x.OpCd == expectedOpCode).FirstOrDefault();
            _mockwiprateRepo.Setup(repo => repo.GetWipRateAsync(expectedOpCode))
                            .ReturnsAsync(expectedWipRate!);

            // Act
            var result = await _controller.GetRateAsync(expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWipRate, okResult.Value);
        }

        [TestMethod]
        public async Task GetRateAsync_NotFoundResult()
        {
            // Arrange
            _mockwiprateRepo.Setup(repo => repo.GetWipRateAsync(expectedOpCode))
                            .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetRateAsync(expectedOpCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion

        #region Wip Rate WorkList Test

        [TestMethod]
        public void GetRateWorkList_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            _mockwiprateRepo.Setup(repo => repo.GetSampleKeys(expectedOpCode,expectedStatusCode))
                            .Returns(expectedWorkList);

            // Act
            var result = _controller.GetRateWorkList(expectedOpCode, expectedStatusCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public void GetRateWorkList_NotFoundResult()
        {
            // Arrange
            _mockwiprateRepo.Setup(repo => repo.GetSampleKeys(expectedOpCode, expectedStatusCode))
                            .Returns(() => null!);

            // Act
            var result = _controller.GetRateWorkList(expectedOpCode, expectedStatusCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetRateWorkListAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedWorkList = GetExpectedWorkList();
            _mockwiprateRepo.Setup(repo => repo.GetSampleKeysAsync(expectedOpCode, expectedStatusCode))
                            .ReturnsAsync(expectedWorkList);

            // Act
            var result = await _controller.GetRateWorkListAsync(expectedOpCode, expectedStatusCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedWorkList, okResult.Value);
        }

        [TestMethod]
        public async Task GetRateWorkListAsync_NotFoundResult()
        {
            // Arrange
            _mockwiprateRepo.Setup(repo => repo.GetSampleKeysAsync(expectedOpCode, expectedStatusCode))
                            .ReturnsAsync(() => null!);

            // Act
            var result = await _controller.GetRateWorkListAsync(expectedOpCode, expectedStatusCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        #endregion
    }
}
