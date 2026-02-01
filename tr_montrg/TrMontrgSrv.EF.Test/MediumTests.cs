using System;
using System.Linq;
using CSG.MI.TrMontrgSrv.EF.Test.Core;
using CSG.MI.TrMontrgSrv.EF.Test.Fixtures;
using CSG.MI.TrMontrgSrv.Helpers.Extensions;
using CSG.MI.TrMontrgSrv.Model;
using Xunit;
using Xunit.Abstractions;

namespace CSG.MI.TrMontrgSrv.EF.Test
{
    public class MediumTests
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
            public void Test_Medium_Get()
            {
                var ymd = "20210807";
                var hms = "111759";
                var deviceId = "TRIOT-A002";
                var mediumType = "temp";
                var expected = _fixture.Media.Single(x => x.Ymd == ymd &&
                                                          x.Hms == hms &&
                                                          x.DeviceId == deviceId &&
                                                          x.MediumType == mediumType);
                using var repo = _fixture.MediumFixture.CreateRepo();
                var actual = repo.Get(ymd, hms, deviceId, mediumType);
                bool isEqual = actual.PublicPropertiesEqual<Medium>(expected, nameof(Medium.UpdatedDt));
                Assert.True(isEqual);
                //Assert.Equal(expected.Ymd, actual.Ymd);
                //Assert.Equal(expected.Hms, actual.Hms);
                //Assert.Equal(expected.DeviceId, actual.DeviceId);
                //Assert.Equal(expected.MediumType, actual.MediumType);
                _output.WriteLine($"expected: YmdHms={expected.Ymd}{expected.Hms}, DeviceId={expected.DeviceId}, MediumType={expected.MediumType}");
                _output.WriteLine($"  actual: YmdHms={actual.Ymd}{actual.Hms}, DeviceId={actual.DeviceId}, MediumType={actual.MediumType}");
            }

            [Fact, TestPriority(2)]
            public void Test_Medium_FindBy_String_With_MediumType()
            {
                var startYmd = "20210801";
                var startHms = "120000";
                var endYmd = "20210807";
                var endHms = "111759";
                var deviceId = "TRIOT-A002";
                var mediumType = "temp";
                var expected = _fixture.Media.Where(x => String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(startYmd, startHms)) >= 0 &&
                                                         String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(endYmd, endHms)) <= 0 &&
                                                         x.DeviceId == deviceId &&
                                                         x.MediumType == mediumType)
                                             .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.MediumType)
                                             .ToList();
                using var repo = _fixture.MediumFixture.CreateRepo();
                var actual = repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId, mediumType)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.MediumType)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Medium>(expected[i], nameof(Medium.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].MediumType, actual[i].MediumType);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, MediumType={expected[i].MediumType}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, MediumType={actual[i].MediumType}");
                }
            }

            [Fact, TestPriority(3)]
            public void Test_Medium_FindBy_String()
            {
                var startYmd = "20210801";
                var startHms = "120000";
                var endYmd = "20210807";
                var endHms = "111759";
                var deviceId = "TRIOT-A002";
                var expected = _fixture.Media.Where(x => String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(startYmd, startHms)) >= 0 &&
                                                         String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                        String.Concat(endYmd, endHms)) <= 0 &&
                                                        x.DeviceId == deviceId)
                                             .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.MediumType)
                                             .ToList();
                using var repo = _fixture.MediumFixture.CreateRepo();
                var actual = repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.MediumType)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Medium>(expected[i], nameof(Medium.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].MediumType, actual[i].MediumType);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, MediumType={expected[i].MediumType}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, MediumType={actual[i].MediumType}");
                }
            }

            [Fact, TestPriority(3)]
            public void Test_Medium_FindBy_DateTime_With_MediumType()
            {
                var startTime = new DateTime(2021, 8, 1, 12, 0, 0);
                var endTime = new DateTime(2021, 8, 7, 11, 17, 59);
                var deviceId = "TRIOT-A002";
                var mediumType = "temp";
                var expected = _fixture.Media.Where(x => x.CaptureDt >= startTime &&
                                                         x.CaptureDt <= endTime &&
                                                         x.DeviceId == deviceId &&
                                                         x.MediumType == mediumType)
                                            .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.MediumType)
                                            .ToList();
                using var repo = _fixture.MediumFixture.CreateRepo();
                var actual = repo.FindBy(startTime, endTime, deviceId, mediumType)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.MediumType)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Medium>(expected[i], nameof(Medium.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].MediumType, actual[i].MediumType);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, MediumType={expected[i].MediumType}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, MediumType={actual[i].MediumType}");
                }
            }
        }
    }
}
