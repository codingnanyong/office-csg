using CSG.MI.FDW.BLL.Feedbacks.Interface;
using CSG.MI.FDW.LoggerService;
using CSG.MI.FDW.OpenApi.Controllers.HQ.v1;
using Microsoft.AspNetCore.Mvc;
using Moq;

namespace CSG.MI.FDW.OpenApi.Test.Feedback
{
    [TestClass]
    public class FeedbackControllerTest
    {
        #region Test-Set

        private Mock<IFeedbackRepo> _mockRepo = null!;
        private Mock<ILoggerManager> _mockLogger = null!;
        private FeedbackController _controller = null!;

        [TestInitialize]
        public void Setup()
        {
            _mockRepo = new Mock<IFeedbackRepo>();
            _mockLogger = new Mock<ILoggerManager>(); 
            _controller = new FeedbackController(_mockRepo.Object, _mockLogger.Object);
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockRepo = null!;
            _mockLogger = null!;
            _controller = null!;
        }

        #endregion

        #region Feedback - Create

        [TestMethod]
        public void Create_NullFeedback_ReturnsBadRequest()
        {
            // Arrange
            CSG.MI.DTO.Feedback.Feedback feedback = null!;

            // Act
            var result = _controller.Create(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public void Create_EmptyCategory_ReturnsBadRequest()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Category = "" };

            // Act
            var result = _controller.Create(feedback);

            // Assert
            var badRequestResult = result as BadRequestObjectResult;
            Assert.IsNotNull(badRequestResult);
            Assert.IsInstanceOfType(badRequestResult.Value, typeof(SerializableError));
        }

        [TestMethod]
        public void Create_InvalidModelState_ReturnsUnprocessableEntity()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Category = "Valid Category" };
            _controller.ModelState.AddModelError("Test", "Test Error");

            // Act
            var result = _controller.Create(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(UnprocessableEntityObjectResult));
        }

        [TestMethod]
        public void Create_ValidFeedback_ReturnsCreated()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Category = "00001" };
            _mockRepo.Setup(repo => repo.CreateFeedback(It.IsAny<CSG.MI.DTO.Feedback.Feedback>()))
                     .Returns(feedback);

            // Act
            var result = _controller.Create(feedback);

            // Assert
            var createdResult = result as CreatedResult;
            Assert.IsNotNull(createdResult);
            Assert.AreEqual("Feedback", createdResult.Location);
            Assert.AreEqual(feedback, createdResult.Value);
        }

        [TestMethod]
        public async Task CreateAsync_NullFeedback_ReturnsBadRequest()
        {
            // Arrange
            CSG.MI.DTO.Feedback.Feedback feedback = null!;

            // Act
            var result = await _controller.CreateAsync(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public async Task CreateAsync_EmptyCategory_ReturnsBadRequest()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Category = "" };

            // Act
            var result = await _controller.CreateAsync(feedback);

            // Assert
            var badRequestResult = result as BadRequestObjectResult;
            Assert.IsNotNull(badRequestResult);
            Assert.IsInstanceOfType(badRequestResult.Value, typeof(SerializableError));
        }

        [TestMethod]
        public async Task CreateAsync_InvalidModelState_ReturnsUnprocessableEntity()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Category = "Valid Category" };
            _controller.ModelState.AddModelError("Test", "Test Error");

            // Act
            var result = await _controller.CreateAsync(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(UnprocessableEntityObjectResult));
        }

        [TestMethod]
        public async Task CreateAsync_ValidFeedback_ReturnsCreated()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Category = "00001" };
            _mockRepo.Setup(repo => repo.CreateFeedbackAsync(It.IsAny<CSG.MI.DTO.Feedback.Feedback>()))
                     .ReturnsAsync(feedback);

            // Act
            var result = await _controller.CreateAsync(feedback);

            // Assert
            var createdResult = result as CreatedResult;
            Assert.IsNotNull(createdResult);
            Assert.AreEqual("Feedback", createdResult.Location);
            Assert.AreEqual(feedback, createdResult.Value);
        }

        [TestMethod]
        public async Task CreateAsync_ValidFeedback_RepoReturnsNull_ReturnsConflict()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Category = "00010" };
            _mockRepo.Setup(repo => repo.CreateFeedbackAsync(It.IsAny<CSG.MI.DTO.Feedback.Feedback>()))
                     .ReturnsAsync((CSG.MI.DTO.Feedback.Feedback)null!);

            // Act
            var result = await _controller.CreateAsync(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(ConflictResult));
        }

        #endregion

        #region Feedback - Read 

        [TestMethod]
        public void GetAll_LangIsNull_ReturnsBadRequest()
        {
            // Arrange
            string lang = null!;

            // Act
            var result = _controller.GetAll(lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public void GetAll_RepoReturnsNull_ReturnsNotFound()
        {
            // Arrange
            string lang = "en";
            _mockRepo.Setup(repo => repo.GetFeedbacks(lang))
                     .Returns((ICollection<CSG.MI.DTO.Feedback.Feedback>)null!);

            // Act
            var result = _controller.GetAll(lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void GetAll_RepoReturnsFeedbackList_ReturnsOk()
        {
            // Arrange
            string lang = "en";
            var feedbackList = new List<CSG.MI.DTO.Feedback.Feedback>
            {
                new CSG.MI.DTO.Feedback.Feedback { Seq = 1, System = "System1" },
                new CSG.MI.DTO.Feedback.Feedback { Seq = 2, System = "System2" }
            };
            _mockRepo.Setup(repo => repo.GetFeedbacks(lang))
                     .Returns(feedbackList);

            // Act
            var result = _controller.GetAll(lang);

            // Assert
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(feedbackList, okResult.Value);
        }

        [TestMethod]
        public async Task GetAllAsync_LangIsNull_ReturnsBadRequest()
        {
            // Arrange
            string lang = null!;

            // Act
            var result = await _controller.GetAllAsync(lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public async Task GetAllAsync_RepoReturnsNull_ReturnsNotFound()
        {
            // Arrange
            string lang = "en";
            _mockRepo.Setup(repo => repo.GetFeedbacksAsync(lang))
                     .ReturnsAsync((ICollection<CSG.MI.DTO.Feedback.Feedback>)null!);

            // Act
            var result = await _controller.GetAllAsync(lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetAllAsync_RepoReturnsFeedbackList_ReturnsOk()
        {
            // Arrange
            string lang = "en";
            var feedbackList = new List<CSG.MI.DTO.Feedback.Feedback>
            {
                new CSG.MI.DTO.Feedback.Feedback { Seq = 1, System = "System1" },
                new CSG.MI.DTO.Feedback.Feedback { Seq = 2, System = "System2" }
            };
            _mockRepo.Setup(repo => repo.GetFeedbacksAsync(lang))
                     .ReturnsAsync(feedbackList);

            // Act
            var result = await _controller.GetAllAsync(lang);

            // Assert
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(feedbackList, okResult.Value);
        }

        [TestMethod]
        public void Get_LangIsNull_ReturnsBadRequest()
        {
            // Arrange
            string lang = null!;

            // Act
            var result = _controller.Get(1, 1, lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public void Get_RepoReturnsNull_ReturnsNotFound()
        {
            // Arrange
            string lang = "en";
            int seq = 1;
            int sys = 1;
            _mockRepo.Setup(repo => repo.GetFeedback(seq, sys, lang))
                     .Returns((CSG.MI.DTO.Feedback.Feedback)null!);

            // Act
            var result = _controller.Get(seq, sys, lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public void Get_RepoReturnsFeedback_ReturnsOk()
        {
            // Arrange
            string lang = "en";
            int seq = 1;
            int sys = 1;
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Seq = seq, System = "System" };
            _mockRepo.Setup(repo => repo.GetFeedback(seq, sys, lang))
                     .Returns(feedback);

            // Act
            var result = _controller.Get(seq, sys, lang);

            // Assert
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(feedback, okResult.Value);
        }

        [TestMethod]
        public async Task GetAsync_LangIsNull_ReturnsBadRequest()
        {
            // Arrange
            string lang = null!;

            // Act
            var result = await _controller.GetAsync(1, 1, lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public async Task GetAsync_RepoReturnsNull_ReturnsNotFound()
        {
            // Arrange
            string lang = "en";
            int seq = 1;
            int sys = 1;
            _mockRepo.Setup(repo => repo.GetFeedbackAsync(seq, sys, lang))
                     .ReturnsAsync((CSG.MI.DTO.Feedback.Feedback)null!);

            // Act
            var result = await _controller.GetAsync(seq, sys, lang);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NotFoundResult));
        }

        [TestMethod]
        public async Task GetAsync_RepoReturnsFeedback_ReturnsOk()
        {
            // Arrange
            string lang = "en";
            int seq = 1;
            int sys = 1;
            var feedback = new CSG.MI.DTO.Feedback.Feedback { Seq = seq, System = "System" };
            _mockRepo.Setup(repo => repo.GetFeedbackAsync(seq, sys, lang))
                     .ReturnsAsync(feedback);

            // Act
            var result = await _controller.GetAsync(seq, sys, lang);

            // Assert
            var okResult = result as OkObjectResult;
            Assert.IsNotNull(okResult);
            Assert.AreEqual(feedback, okResult.Value);
        }

        #endregion

        #region Feedback - Update

        [TestMethod]
        public void Update_NullFeedback_ReturnsBadRequest()
        {
            // Arrange
            CSG.MI.DTO.Feedback.Feedback feedback = null!;

            // Act
            var result = _controller.Update(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public void Update_EmptySystemAndNegativeSeq_ReturnsBadRequest()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "", Seq = -1 };

            // Act
            var result = _controller.Update(feedback);

            // Assert
            var badRequestResult = result as BadRequestObjectResult;
            Assert.IsNotNull(badRequestResult);
            Assert.IsInstanceOfType(badRequestResult.Value, typeof(SerializableError));
            Assert.IsTrue(_controller.ModelState.ContainsKey("Feedback_Seq"));
        }

        [TestMethod]
        public void Update_InvalidModelState_ReturnsUnprocessableEntity()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "Valid System", Seq = 1 };
            _controller.ModelState.AddModelError("Test", "Test Error");

            // Act
            var result = _controller.Update(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(UnprocessableEntityObjectResult));
        }

        [TestMethod]
        public void Update_ValidFeedback_ReturnsNoContent()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "Valid System", Seq = 1 };
            _mockRepo.Setup(repo => repo.UpdateFeedback(It.IsAny<CSG.MI.DTO.Feedback.Feedback>()))
                     .Returns(feedback);

            // Act
            var result = _controller.Update(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NoContentResult));
        }

        [TestMethod]
        public void Update_ValidFeedback_RepoReturnsNull_ReturnsConflict()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "Valid System", Seq = 1 };
            _mockRepo.Setup(repo => repo.UpdateFeedback(It.IsAny<CSG.MI.DTO.Feedback.Feedback>()))
                     .Returns((CSG.MI.DTO.Feedback.Feedback)null!);

            // Act
            var result = _controller.Update(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(ConflictResult));
        }

        [TestMethod]
        public async Task UpdateAsync_NullFeedback_ReturnsBadRequest()
        {
            // Arrange
            CSG.MI.DTO.Feedback.Feedback feedback = null!;

            // Act
            var result = await _controller.UpdateAsync(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public async Task UpdateAsync_EmptySystemAndNegativeSeq_ReturnsBadRequest()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "", Seq = -1 };

            // Act
            var result = await _controller.UpdateAsync(feedback);

            // Assert
            var badRequestResult = result as BadRequestObjectResult;
            Assert.IsNotNull(badRequestResult);
            Assert.IsInstanceOfType(badRequestResult.Value, typeof(SerializableError));
            Assert.IsTrue(_controller.ModelState.ContainsKey("Feedback_Seq"));
        }

        [TestMethod]
        public async Task UpdateAsync_InvalidModelState_ReturnsUnprocessableEntity()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "Valid System", Seq = 1 };
            _controller.ModelState.AddModelError("Test", "Test Error");

            // Act
            var result = await _controller.UpdateAsync(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(UnprocessableEntityObjectResult));
        }

        [TestMethod]
        public async Task UpdateAsync_ValidFeedback_ReturnsNoContent()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "Valid System", Seq = 1 };
            _mockRepo.Setup(repo => repo.UpdateFeedbackAsync(It.IsAny<CSG.MI.DTO.Feedback.Feedback>()))
                     .ReturnsAsync(feedback);

            // Act
            var result = await _controller.UpdateAsync(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NoContentResult));
        }

        [TestMethod]
        public async Task UpdateAsync_ValidFeedback_RepoReturnsNull_ReturnsConflict()
        {
            // Arrange
            var feedback = new CSG.MI.DTO.Feedback.Feedback { System = "Valid System", Seq = 1 };
            _mockRepo.Setup(repo => repo.UpdateFeedbackAsync(It.IsAny<CSG.MI.DTO.Feedback.Feedback>()))
                     .ReturnsAsync((CSG.MI.DTO.Feedback.Feedback)null!);

            // Act
            var result = await _controller.UpdateAsync(feedback);

            // Assert
            Assert.IsInstanceOfType(result, typeof(ConflictResult));
        }

        #endregion

        #region Feedback - Delete

        [TestMethod]
        public void Delete_NegativeSeq_ReturnsBadRequest()
        {
            // Act
            var result = _controller.Delete(-1, 1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public void Delete_NegativeSys_ReturnsBadRequest()
        {
            // Act
            var result = _controller.Delete(1, -1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public void Delete_RepoReturnsZero_ReturnsConflict()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.Delete(It.IsAny<int>(), It.IsAny<int>())).Returns(0);

            // Act
            var result = _controller.Delete(1, 1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(ConflictResult));
        }

        [TestMethod]
        public void Delete_ValidParameters_ReturnsNoContent()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.Delete(It.IsAny<int>(), It.IsAny<int>())).Returns(1);

            // Act
            var result = _controller.Delete(1, 1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NoContentResult));
        }

        [TestMethod]
        public async Task DeletAsync_NegativeSeq_ReturnsBadRequest()
        {
            // Act
            var result = await _controller.DeleteAsync(-1, 1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public async Task DeleteAsync_NegativeSys_ReturnsBadRequest()
        {
            // Act
            var result = await _controller.DeleteAsync(1, -1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(BadRequestResult));
        }

        [TestMethod]
        public async Task DeleteAsync_RepoReturnsZero_ReturnsConflict()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.DeleteAsync(It.IsAny<int>(), It.IsAny<int>())).ReturnsAsync(0);

            // Act
            var result = await _controller.DeleteAsync(1, 1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(ConflictResult));
        }

        [TestMethod]
        public async Task DeleteAsync_ValidParameters_ReturnsNoContent()
        {
            // Arrange
            _mockRepo.Setup(repo => repo.DeleteAsync(It.IsAny<int>(), It.IsAny<int>())).ReturnsAsync(1);

            // Act
            var result = await _controller.DeleteAsync(1, 1);

            // Assert
            Assert.IsInstanceOfType(result, typeof(NoContentResult));
        }

        #endregion
    }
}
