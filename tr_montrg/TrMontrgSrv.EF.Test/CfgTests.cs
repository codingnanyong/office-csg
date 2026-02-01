using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Test.Fixtures;
using CSG.MI.TrMontrgSrv.Helpers.Extensions;
using CSG.MI.TrMontrgSrv.Model;
using Xunit;
using Xunit.Abstractions;

namespace CSG.MI.TrMontrgSrv.EF.Test
{
    public class CfgTests
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

            //[Fact]
            //public void Test_Cfg_Get()
            //{
            //    var deviceId = "TRIOT-A001";

            //    var expected = _fixture.Cfgs.Where(x => x.DeviceId == deviceId)
            //                                .OrderByDescending(x => x.Id)
            //                                .SingleOrDefault();

            //    using var repo = _fixture.CfgFixture.CreateRepo();
            //    var actual = repo.GetLast(deviceId);

            //    bool isEqual = actual.PublicPropertiesEqual<Cfg>(expected, nameof(Cfg.Id), nameof(Cfg.UpdatedDt));
            //    Assert.True(isEqual);
            //    _output.WriteLine($"expected: Id={expected.Id}, YmdHms={expected.Ymd}{expected.Hms}, DeviceId={expected.DeviceId}");
            //    _output.WriteLine($"  actual: Id={actual.Id}, YmdHms={actual.Ymd}{actual.Hms}, DeviceId={actual.DeviceId}");
            //}

            [Fact]
            public void Test_Cfg_FindBy()
            {
                var deviceId = "TRIOT-A002";

                var expected = _fixture.Cfgs.Where(x => x.DeviceId == deviceId)
                                            .OrderByDescending(x => x.Id)
                                            .ToList();

                using var repo = _fixture.CfgFixture.CreateRepo();
                var actual = repo.FindBy(deviceId)
                                 .OrderByDescending(x => x.Id)
                                 .ToList();

                Assert.Equal(expected.Count, actual.Count);
                _output.WriteLine($"count => expected: {expected.Count}, acutal: {actual.Count}");

                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Cfg>(expected[i], nameof(Cfg.Id), nameof(Cfg.UpdatedDt));
                    Assert.True(isEqual);
                    _output.WriteLine($"expected: Id={expected[i].Id}, YmdHms={expected[i].Ymd}{expected[i].Hms}, DeviceId={expected[i].DeviceId}");
                    _output.WriteLine($"  actual: Id={actual[i].Id}, YmdHms={actual[i].Ymd}{actual[i].Hms}, DeviceId={actual[i].DeviceId}");
                }
            }

            //[Fact]
            //public void Test_Cfg_PropertiesEquality()
            //{
            //    var deviceId = "TRIOT-A001";

            //    var expected = _fixture.Cfgs.Where(x => x.DeviceId == deviceId)
            //                                .OrderByDescending(x => x.Id)
            //                                .SingleOrDefault();

            //    using var repo = _fixture.CfgFixture.CreateRepo();
            //    var actual = repo.GetLast(deviceId);

            //    bool isEqual = actual.PublicPropertiesEqual<Cfg>(expected, nameof(Cfg.Id), nameof(Cfg.UpdatedDt));
            //    Assert.True(isEqual);
            //    _output.WriteLine($"expected: Id={expected.Id}, YmdHms={expected.Ymd}{expected.Hms}, DeviceId={expected.DeviceId}");
            //    _output.WriteLine($"  actual: Id={actual.Id}, YmdHms={actual.Ymd}{actual.Hms}, DeviceId={actual.DeviceId}");
            //}

            //[Fact]
            //public void Test_Cfg_InstanceEquality()
            //{
            //    var deviceId = "TRIOT-A001";

            //    var expected = _fixture.Cfgs.Where(x => x.DeviceId == deviceId)
            //                                .OrderByDescending(x => x.Id)
            //                                .SingleOrDefault();

            //    using var repo = _fixture.CfgFixture.CreateRepo();
            //    var actual = repo.GetLast(deviceId);
            //    bool isEqual = (expected = actual)
            //}

        }

    }
}
