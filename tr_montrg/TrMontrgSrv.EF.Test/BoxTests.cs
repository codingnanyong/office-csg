using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Test.Core;
using CSG.MI.TrMontrgSrv.EF.Test.Fixtures;
using CSG.MI.TrMontrgSrv.Helpers.Extensions;
using CSG.MI.TrMontrgSrv.Model;
using Xunit;
using Xunit.Abstractions;

namespace CSG.MI.TrMontrgSrv.EF.Test
{
    public class BoxTests
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
            public void Test_Box_Get()
            {
                var ymd = "20210807";
                var hms = "111759";
                var deviceId = "TRIOT-A002";
                var boxId = 0;
                var expected = _fixture.Boxes.Single(x => x.Ymd == ymd &&
                                                          x.Hms == hms &&
                                                          x.DeviceId == deviceId &&
                                                          x.BoxId == boxId);
                using var repo = _fixture.BoxFixture.CreateRepo();
                var actual = repo.Get(ymd, hms, deviceId, boxId);
                bool isEqual = actual.PublicPropertiesEqual<Box>(expected, nameof(Box.UpdatedDt));
                Assert.True(isEqual);
                //Assert.Equal(expected.Ymd, actual.Ymd);
                //Assert.Equal(expected.Hms, actual.Hms);
                //Assert.Equal(expected.DeviceId, actual.DeviceId);
                //Assert.Equal(expected.BoxId, actual.BoxId);
                _output.WriteLine($"expected: YmdHms={expected.Ymd}{expected.Hms}, DeviceId={expected.DeviceId}, BoxId={expected.BoxId}");
                _output.WriteLine($"  actual: YmdHms={actual.Ymd}{actual.Hms}, DeviceId={actual.DeviceId}, BoxId={actual.BoxId}");
            }

            [Fact, TestPriority(2)]
            public void Test_Box_FindBy_String_With_BoxId()
            {
                var startYmd = "20210801";
                var startHms = "120000";
                var endYmd = "20210807";
                var endHms = "111759";
                var deviceId = "TRIOT-A002";
                var boxId = 0;
                var expected = _fixture.Boxes.Where(x => String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(startYmd, startHms)) >= 0 &&
                                                         String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(endYmd, endHms)) <= 0 &&
                                                         x.DeviceId == deviceId &&
                                                         x.BoxId == boxId)
                                             .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.BoxId)
                                             .ToList();
                using var repo = _fixture.BoxFixture.CreateRepo();
                var actual = repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId, boxId)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.BoxId)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Box>(expected[i], nameof(Box.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].BoxId, actual[i].BoxId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, BoxId={expected[i].BoxId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, BoxId={actual[i].BoxId}");
                }
            }

            [Fact, TestPriority(3)]
            public void Test_Box_FindBy_String()
            {
                var startYmd = "20210801";
                var startHms = "120000";
                var endYmd = "20210807";
                var endHms = "111759";
                var deviceId = "TRIOT-A002";
                var expected = _fixture.Boxes.Where(x => String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(startYmd, startHms)) >= 0 &&
                                                         String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(endYmd, endHms)) <= 0 &&
                                                        x.DeviceId == deviceId)
                                             .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.BoxId)
                                             .ToList();
                using var repo = _fixture.BoxFixture.CreateRepo();
                var actual = repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.BoxId)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Box>(expected[i], nameof(Box.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].BoxId, actual[i].BoxId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, BoxId={expected[i].BoxId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, BoxId={actual[i].BoxId}");
                }
            }

            [Fact, TestPriority(3)]
            public void Test_Box_FindBy_DateTime_With_BoxId()
            {
                var startTime = new DateTime(2021, 8, 1, 12, 0, 0);
                var endTime = new DateTime(2021, 8, 7, 11, 17, 59);
                var deviceId = "TRIOT-A002";
                var boxId = 0;
                var expected = _fixture.Boxes.Where(x => x.CaptureDt >= startTime &&
                                                         x.CaptureDt <= endTime &&
                                                         x.DeviceId == deviceId &&
                                                         x.BoxId == boxId)
                                             .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.BoxId)
                                             .ToList();
                using var repo = _fixture.BoxFixture.CreateRepo();
                var actual = repo.FindBy(startTime, endTime, deviceId, boxId)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.BoxId)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Box>(expected[i], nameof(Box.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].BoxId, actual[i].BoxId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, BoxId={expected[i].BoxId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, BoxId={actual[i].BoxId}");
                }
            }

        }
    }
}
