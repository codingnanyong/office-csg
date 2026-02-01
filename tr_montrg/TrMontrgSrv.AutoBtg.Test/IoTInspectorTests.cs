using CSG.MI.TrMontrgSrv.AutoBtg.Generator.Interface;
using CSG.MI.TrMontrgSrv.AutoBtg.Inspector;
using CSG.MI.TrMontrgSrv.AutoBtg.PingCore.Interface;
using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using Moq;
using Moq.Protected;
using System.Net;

namespace CSG.MI.TrMontrgSrv.AutoBtg.Test
{
    [TestClass]
    public class IoTInspectorTests
    {
        #region GetDatas Method Test

        [TestMethod]
        public async Task GetDatas_Normal()
        {
            var batchGeneratorMock = new Mock<IBatchGenerator<InspecDevice>>();
            var pingWrapperMock = new Mock<IPingWrapper>();

            var inspector = new IoTInspector(batchGeneratorMock.Object, pingWrapperMock.Object);

            var httpClientHandlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            httpClientHandlerMock
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent("[{\"DeviceId\": \"TestDevice\",\"IpAddress\": \"127.0.0.1\"}]"),
                });

            var httpClient = new HttpClient(httpClientHandlerMock.Object);
            inspector.HttpClient = httpClient;

            var devices = await inspector.GetDatas();

            Assert.IsNotNull(devices);
            Assert.AreEqual(0, devices.Count);
        }


        [TestMethod]
        public async Task GetDatas_Error()
        {
            var batchGeneratorMock = new Mock<IBatchGenerator<InspecDevice>>();
            var pingWrapperMock = new Mock<IPingWrapper>();

            var inspector = new IoTInspector(batchGeneratorMock.Object, pingWrapperMock.Object);

            var httpClientHandlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);
            httpClientHandlerMock
                .Protected()
                .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
                .ReturnsAsync(new HttpResponseMessage
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent("[{\"DeviceId\": \"TestDevice\",\"IpAddress\": \"127.0.0.1\"}]"),
                });

            var httpClient = new HttpClient(httpClientHandlerMock.Object);
            inspector.HttpClient = httpClient;

            var devices = await inspector.GetDatas();
            devices.Add(new InspecDevice { DeviceId = "TestDevice", IpAddress = "127.0.0.1" });

            Assert.IsNotNull(devices);
            Assert.AreNotEqual(0, devices.Count);
        }

        #endregion
    }
}
