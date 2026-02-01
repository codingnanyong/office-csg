using CSG.MI.DTO.Feedback;
using CSG.MI.FDW.BLL.Feedbacks.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Feedback
{
    [TestClass]
    public class CategoryControllerTest
    {
        private Mock<ICategoryRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private CategoryController _controller = null!;

        private List<Category> GetExpectedCategories()
        {
            return new List<Category>()
            {
                new Category() { Id = "00001", Value = "FEATURE REQUESTS" },
                new Category() { Id = "00002", Value = "BUG REPORTS" },
                new Category() { Id = "00003", Value = "HOW TO" },
                new Category() { Id = "00004", Value = "ETC" }
            };
        }

        protected const string expectedOkCode = "en";
        protected const string expectedFailCode = "te";

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<ICategoryRepo>();
            _mockLogger = new Mock<ILoggerManager>();
            _controller = new CategoryController(_mockRepo.Object, _mockLogger.Object);
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
            var expectedCategories = GetExpectedCategories();
            _mockRepo.Setup(repo => repo.Get(expectedOkCode))
                     .Returns(expectedCategories);
            
            // Act
            var result = _controller.GetAll(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedCategories, okResult.Value);
        }

        [TestMethod]
        public void GetAll_ReturnsNotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.Get(expectedFailCode))
                     .Returns(() => null!);

            // Act
            var result = _controller.GetAll(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetAllAsync_ReturnsOkResult()
        {
            // Arrange
            var expectedCategories = GetExpectedCategories();
            _mockRepo.Setup(repo => repo.GetAsync(expectedOkCode))
                     .ReturnsAsync(expectedCategories);

            // Act
            var result = await _controller.GetAllAsync(expectedOkCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(OkObjectResult));
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(expectedCategories, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_ReturnsNotFoundResult()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.GetAsync(expectedFailCode))
                     .ReturnsAsync(() =>null!);

            // Act
            var result = await _controller.GetAllAsync(expectedFailCode);

            // Assert
            Assert.IsInstanceOfType(result, typeof(IActionResult));
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }
    }
}
