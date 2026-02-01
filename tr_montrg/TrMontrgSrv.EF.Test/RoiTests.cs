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
using Xunit;
using Xunit.Abstractions;

namespace CSG.MI.TrMontrgSrv.EF.Test
{
    public class RoiTests
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
            public void Test_Roi_Get()
            {
                var ymd = "20210807";
                var hms = "111759";
                var deviceId = "TRIOT-A002";
                var roiId = 0;
                var expected = _fixture.Rois.Single(x => x.Ymd == ymd &&
                                                         x.Hms == hms &&
                                                         x.DeviceId == deviceId &&
                                                         x.RoiId == roiId);
                using var repo = _fixture.RoiFixture.CreateRepo();
                var actual = repo.Get(ymd, hms, deviceId, roiId);
                bool isEqual = actual.PublicPropertiesEqual<Roi>(expected, nameof(Roi.UpdatedDt));
                Assert.True(isEqual);
                //Assert.Equal(expected.Ymd, actual.Ymd);
                //Assert.Equal(expected.Hms, actual.Hms);
                //Assert.Equal(expected.DeviceId, actual.DeviceId);
                //Assert.Equal(expected.RoiId, actual.RoiId);
                _output.WriteLine($"expected: YmdHms={expected.Ymd}{expected.Hms}, DeviceId={expected.DeviceId}, RoiId={expected.RoiId}");
                _output.WriteLine($"  actual: YmdHms={actual.Ymd}{actual.Hms}, DeviceId={actual.DeviceId}, RoiId={actual.RoiId}");
            }

            [Fact, TestPriority(2)]
            public void Test_Roi_FindBy_String_With_RoiId()
            {
                var startYmd = "20210801";
                var startHms = "120000";
                var endYmd = "20210807";
                var endHms = "111759";
                var deviceId = "TRIOT-A002";
                var roiId = 0;
                var expected = _fixture.Rois.Where(x => String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                       String.Concat(startYmd, startHms)) >= 0 &&
                                                        String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                       String.Concat(endYmd, endHms)) <= 0 &&
                                                        x.DeviceId == deviceId &&
                                                        x.RoiId == roiId)
                                            .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.RoiId)
                                            .ToList();
                using var repo = _fixture.RoiFixture.CreateRepo();
                var actual = repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId, roiId)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.RoiId)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Roi>(expected[i], nameof(Roi.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].RoiId, actual[i].RoiId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, RoiId={expected[i].RoiId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, RoiId={actual[i].RoiId}");
                }
            }

            [Fact, TestPriority(3)]
            public void Test_Roi_FindBy_String()
            {
                var startYmd = "20210801";
                var startHms = "120000";
                var endYmd = "20210807";
                var endHms = "111759";
                var deviceId = "TRIOT-A002";
                var expected = _fixture.Rois.Where(x => String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                       String.Concat(startYmd, startHms)) >= 0 &&
                                                        String.Compare(String.Concat(x.Ymd, x.Hms),
                                                                       String.Concat(endYmd, endHms)) <= 0 &&
                                                        x.DeviceId == deviceId)
                                            .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.RoiId)
                                            .ToList();
                using var repo = _fixture.RoiFixture.CreateRepo();
                var actual = repo.FindBy(startYmd, startHms, endYmd, endHms, deviceId)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.RoiId)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Roi>(expected[i], nameof(Roi.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].RoiId, actual[i].RoiId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, RoiId={expected[i].RoiId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, RoiId={actual[i].RoiId}");
                }
            }

            [Fact, TestPriority(3)]
            public void Test_Roi_FindBy_DateTime_With_RoiId()
            {
                var startTime = new DateTime(2021, 8, 1, 12, 0, 0);
                var endTime = new DateTime(2021, 8, 7, 11, 17, 59);
                var deviceId = "TRIOT-A002";
                var roiId = 0;
                var expected = _fixture.Rois.Where(x => x.CaptureDt >= startTime &&
                                                        x.CaptureDt <= endTime &&
                                                        x.DeviceId == deviceId &&
                                                        x.RoiId == roiId)
                                            .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.RoiId)
                                            .ToList();
                using var repo = _fixture.RoiFixture.CreateRepo();
                var actual = repo.FindBy(startTime, endTime, deviceId, roiId)
                                 .OrderBy(x => x.Ymd).ThenBy(x => x.Hms).ThenBy(x => x.DeviceId).ThenBy(x => x.RoiId)
                                 .ToList();
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");
                Assert.Equal(expected.Count, actual.Count);

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Roi>(expected[i], nameof(Roi.UpdatedDt));
                    Assert.True(isEqual);
                    //Assert.Equal(expected[i].Ymd, actual[i].Ymd);
                    //Assert.Equal(expected[i].Hms, actual[i].Hms);
                    //Assert.Equal(expected[i].DeviceId, actual[i].DeviceId);
                    //Assert.Equal(expected[i].RoiId, actual[i].RoiId);
                    _output.WriteLine($"expected: YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}, RoiId={expected[i].RoiId}");
                    _output.WriteLine($"  actual: YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}, RoiId={actual[i].RoiId}");
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
                var roiId = 0;

                var expected = 1;

                using var repo = _fixture.RoiFixture.CreateRepo();
                List<ImrData> list = repo.GetImrChartData(startYmd, startHms, endYmd, endHms, deviceId, roiId);
                var actual = list.Count;

                Assert.Equal(expected, actual);
                _output.WriteLine($"");

            }
        }
    }
}
