using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Test.Core;
using CSG.MI.TrMontrgSrv.EF.Test.Fixtures;
using CSG.MI.TrMontrgSrv.Helpers.Extensions;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using CSG.MI.TrMontrgSrv.Model.Json;
using Xunit;
using Xunit.Abstractions;

namespace CSG.MI.TrMontrgSrv.EF.Test
{
    public class FrameTests
    {
        [Collection("Database")]
        [TestCaseOrderer("CSG.MI.TrMontrgSrv.EF.Test.Core.PriorityOrderer", "TrMontrgSrv.EF.Test")]
        public class GetFindTests
        {
            private readonly DatabaseFixture _fixture;
            private readonly ITestOutputHelper _output;

            public GetFindTests(DatabaseFixture fixture, ITestOutputHelper output)
            {
                _fixture = fixture;
                _output = output;
            }

            [Fact, TestPriority(1)]
            public void Test_Frame_Get()
            {
                var ymd = "20210807";
                var hms = "111759";
                var deviceId = "TRIOT-A002";
                var expected = _fixture.Frames.Single(x => x.Ymd == ymd &&
                                                           x.Hms == hms &&
                                                           x.DeviceId == deviceId);
                using var repo = _fixture.FrameFixture.CreateRepo();
                var actual = repo.Get(ymd, hms, deviceId);
                bool isEqual = actual.PublicPropertiesEqual<Frame>(expected, nameof(Frame.UpdatedDt));
                Assert.True(isEqual);
                _output.WriteLine($"expected: YmdHms={expected.Ymd}{expected.Hms}, DeviceId={expected.DeviceId}");
                _output.WriteLine($"  actual: YmdHms={actual.Ymd}{actual.Hms}, DeviceId={actual.DeviceId}");
            }

            [Fact, TestPriority(2)]
            public void Test_Frame_FindBy_String()
            {
                var startYmd = "20210807";
                var startHms = "120000";
                var endYmd = "20210807";
                var endHms = "111759";
                var deviceId = "TRIOT-A002";
                var expected = _fixture.Frames.Where(x => String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                         String.Concat(startYmd, startHms)) >= 0 &&
                                                          String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                         String.Concat(endYmd, endHms)) <= 0 &&
                                                          x.DeviceId == deviceId)
                                              .OrderBy(x => new { x.Ymd, x.Hms, x.DeviceId })
                                              .ToList();
                using var repo = _fixture.FrameFixture.CreateRepo();
                var actual = repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId)
                                 .OrderBy(x => new { x.Ymd, x.Hms, x.DeviceId })
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Frame>(expected[i], nameof(Frame.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}");
                }
            }

            [Fact, TestPriority(3)]
            public void Test_Frame_FindBy_DateTime()
            {
                var startTime = "20210807_110000".ToDateTime().Value;
                var endTime = "20210807_111759".ToDateTime().Value;
                var deviceId = "TRIOT-A001";
                var expected = _fixture.Frames.Where(x => x.CaptureDt >= startTime &&
                                                          x.CaptureDt <= endTime &&
                                                          x.DeviceId == deviceId)
                                              .OrderBy(x => new { x.Ymd, x.Hms })
                                              .ToList();
                using var repo = _fixture.FrameFixture.CreateRepo();
                var actual = repo.FindBy(startTime, endTime, deviceId)
                                 .OrderBy(x => new { x.Ymd, x.Hms })
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Frame>(expected[i], nameof(Frame.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}");
                }
            }


        }

        [Collection("Database")]
        [TestCaseOrderer("CSG.MI.TrMontrgSrv.EF.Test.Core.PriorityOrderer", "TrMontrgSrv.EF.Test")]
        public class AddDeleteTests
        {
            private readonly DatabaseFixture _fixture;
            private readonly ITestOutputHelper _output;

            public AddDeleteTests(DatabaseFixture fixture, ITestOutputHelper output)
            {
                _fixture = fixture;
                _output = output;
            }

            [Fact, TestPriority(1)]
            public void Test_Frame_Add()
            {
                var ymd = "20210807";
                var hms = "111759";
                var deviceId = "TRIOT-A002";
                var expected = _fixture.Frames.Single(x => x.Ymd == ymd &&
                                                           x.Hms == hms &&
                                                           x.DeviceId == deviceId);
                expected.Ymd = "20210807";
                expected.Hms = "070900";
                expected.CaptureDt = $"{expected.Ymd}{expected.Hms}".ToDateTime();
                using var repo = _fixture.FrameFixture.CreateRepo();
                repo.Add(expected);
                var actual = repo.Get(expected.Ymd, expected.Hms, expected.DeviceId);
                bool isEqual = actual.PublicPropertiesEqual<Frame>(expected, nameof(Frame.UpdatedDt));
                Assert.True(isEqual);
                //Assert.Equal(expected.Ymd, actual.Ymd);
                //Assert.Equal(expected.Hms, actual.Hms);
                //Assert.Equal(expected.DeviceId, actual.DeviceId);
                _output.WriteLine($"expected: YmdHms={expected.Ymd}{expected.Hms}, DeviceId={expected.DeviceId}");
                _output.WriteLine($"  actual: YmdHms={actual.Ymd}{actual.Hms}, DeviceId={actual.DeviceId}");
            }

            [Fact, TestPriority(2)]
            public void Test_Frame_Delete()
            {
                var ymd = "20210807";
                var hms = "070900";
                var deviceId = "TRIOT-A002";
                Frame frame = null;

                using (var repo = _fixture.FrameFixture.CreateRepo())
                {
                    frame = repo.Get(ymd, hms, deviceId);
                }

                using (var repo = _fixture.FrameFixture.CreateRepo())
                {
                    int expected = 1;
                    int actual = repo.Delete(frame);
                    Assert.Equal(expected, actual);
                }

                using (var repo = _fixture.FrameFixture.CreateRepo())
                {
                    frame = repo.Get(ymd, hms, deviceId);
                    Assert.Null(frame);
                }
            }
        }

        [Collection("Database")]
        [TestCaseOrderer("CSG.MI.TrMontrgSrv.EF.Test.Core.PriorityOrderer", "TrMontrgSrv.EF.Test")]
        public class StoredProcedureTests
        {
            private readonly DatabaseFixture _fixture;
            private readonly ITestOutputHelper _output;

            public StoredProcedureTests(DatabaseFixture fixture, ITestOutputHelper output)
            {
                _fixture = fixture;
                _output = output;
            }

            [Fact, TestPriority(1)]
            public void Test_Frame_GetImrChartData()
            {
                var startYmd = "20210807";
                var startHms = "000000";
                var endYmd = "20210807";
                var endHms = "235959";
                var deviceId = "TRIOT-A001";

                var expected = 1;

                using var repo = _fixture.FrameFixture.CreateRepo();
                List<ImrData> list = repo.GetImrChartData(deviceId, startYmd, startHms, endYmd, endHms);
                var actual = list.Count;

                Assert.Equal(expected, actual);
            }
        }
    }
}
