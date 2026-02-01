using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.EF.Test.Core;
using CSG.MI.TrMontrgSrv.EF.Test.Fixtures;
using CSG.MI.TrMontrgSrv.Helpers.Extensions;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.Json;
using Microsoft.EntityFrameworkCore;
using Xunit;
using Xunit.Abstractions;

//[assembly: TestCollectionOrderer("CSG.MI.TrMontrgSrv.EF.Test.Core.PriorityOrderer", "TrMontrgSrv.EF.Test")]
// Need to turn off test parallelization so we can validate the run order
//[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace CSG.MI.TrMontrgSrv.EF.Test
{
    public class DeviceTests
    {
        [Collection("Database")]
        [TestCaseOrderer("CSG.MI.TrMontrgSrv.EF.Test.Core.PriorityOrderer", "TrMontrgSrv.EF.Test")]
        public class BasicTests
        {
            private readonly DatabaseFixture _fixture;
            private readonly ITestOutputHelper _output;

            public BasicTests(DatabaseFixture fixture, ITestOutputHelper output)
            {
                _fixture = fixture;
                _output = output;
            }

            [Fact, TestPriority(1)]
            public void Test_Database_ConnString_NotEmpty()
            {
                var actual = _fixture.DeviceFixture.Context.Database.GetDbConnection().ConnectionString;
                _output.WriteLine($@"ConnString: {actual}");
                Assert.NotNull(actual);
            }

            [Fact, TestPriority(2)]
            public void Test_Database_Existence()
            {
                var ctx = _fixture.DeviceFixture.Context;
                //Assert.False(ctx.Database.EnsureDeleted());
                Assert.False(ctx.Database.EnsureCreated());
            }

            [Fact, TestPriority(3)]
            public void Test_Database_CanConnect()
            {
                var expected = true;
                var actual = _fixture.DeviceFixture.Context.Database.CanConnect();
                Assert.Equal(expected, actual);
            }
        }

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
            public void Test_Device_GetAll()
            {
                using var repo = _fixture.DeviceFixture.CreateRepo();
                var expected = _fixture.Devices.OrderBy(x => x.DeviceId)
                                               .ToList();
                var actual = repo.GetAll().OrderBy(x => x.DeviceId)
                                          .ToList();
                //Assert.Collection(
                //    actual,
                //    x => Assert.Contains(expected[0].DeviceId, x.DeviceId),
                //    x => Assert.Contains(expected[1].DeviceId, x.DeviceId)
                //);
                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Device>(expected[i], nameof(Device.UpdatedDt));
                    Assert.True(isEqual);
                }
            }

            [Fact, TestPriority(1)]
            public void Test_Device_Get()
            {
                using var repo = _fixture.DeviceFixture.CreateRepo();
                var expected = _fixture.Devices.First();
                var actual = repo.Get(expected.DeviceId);
                _output.WriteLine($"Total Frames: {actual.Frames.Count}");
                _output.WriteLine($"Total Rois: {actual.Rois.Count}");
                _output.WriteLine($"Total Boxes: {actual.Boxes.Count}");
                _output.WriteLine($"Total Cfgs: {actual.Cfgs.Count}");
                //Assert.Equal(expected.DeviceId, actual.DeviceId);
                bool isEqual = actual.PublicPropertiesEqual<Device>(expected, nameof(Device.UpdatedDt));
                Assert.True(isEqual);
            }

            [Fact, TestPriority(1)]
            public void Test_Device_FindAllBy_PlantId()
            {
                var plantId = "JJ";
                using var repo = _fixture.DeviceFixture.CreateRepo();
                var expected = _fixture.Devices.Where(x => x.PlantId == plantId)
                                               .OrderBy(x => x.DeviceId)
                                               .ToList();
                //var actual = repo.FindBy(x => x.PlantId == "JJ");
                var actual = repo.FindAll(x => x.PlantId == plantId)
                                 .OrderBy(x => x.DeviceId)
                                 .ToList();
                actual.ToList().ForEach(x =>
                {
                    _output.WriteLine($"id = {x.DeviceId}");
                    _output.WriteLine($"- Total Frames: {x.Frames.Count}");
                    _output.WriteLine($"- Total Rois: {x.Rois.Count}");
                    _output.WriteLine($"- Total Boxes: {x.Boxes.Count}");
                    _output.WriteLine($"- Total Cfgs: {x.Cfgs.Count}");
                });

                //Assert.Collection(
                //    actual,
                //    x => Assert.Contains(expected[0].DeviceId, x.DeviceId),
                //    x => Assert.Contains(expected[1].DeviceId, x.DeviceId)
                //);
                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Device>(expected[i], nameof(Device.UpdatedDt));
                    Assert.True(isEqual);
                }
            }

            [Fact, TestPriority(1)]
            public void Test_Device_FindAllBy_PlantId_LocationId()
            {
                string plantId = "JJ";
                string locationId = "TR01";
                using var repo = _fixture.DeviceFixture.CreateRepo();
                var expected = _fixture.Devices.Where(x => x.PlantId == plantId && x.LocationId == locationId)
                                               .OrderBy(x => x.DeviceId)
                                               .ToList();
                var actual = repo.FindAll(x => x.PlantId == plantId && x.LocationId == locationId)
                                 .OrderBy(x => x.DeviceId)
                                 .ToList();
                actual.ToList().ForEach(x =>
                {
                    _output.WriteLine($"id = {x.DeviceId}");
                    _output.WriteLine($"- Total Frames: {x.Frames.Count}");
                    _output.WriteLine($"- Total Rois: {x.Rois.Count}");
                    _output.WriteLine($"- Total Boxes: {x.Boxes.Count}");
                    _output.WriteLine($"- Total Cfgs: {x.Cfgs.Count}");
                });
                //Assert.Collection(
                //    actual,
                //    x => Assert.Contains(expected[1].DeviceId, x.DeviceId)
                //);
                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Device>(expected[i], nameof(Device.UpdatedDt));
                    Assert.True(isEqual);
                }
            }

            [Fact, TestPriority(1)]
            public void Test_Device_FindBy_PlantId()
            {
                string plantId = "JJ";
                using var repo = _fixture.DeviceFixture.CreateRepo();
                var expected = _fixture.Devices.Where(x => x.PlantId == plantId)
                                               .OrderBy(x => x.DeviceId)
                                               .ToList();
                var actual = repo.FindBy(plantId).ToList();
                actual.ToList().ForEach(x =>
                {
                    _output.WriteLine($"id = {x.DeviceId}");
                    _output.WriteLine($"- Total Frames: {x.Frames.Count}");
                    _output.WriteLine($"- Total Rois: {x.Rois.Count}");
                    _output.WriteLine($"- Total Boxes: {x.Boxes.Count}");
                    _output.WriteLine($"- Total Cfgs: {x.Cfgs.Count}");
                });
                //Assert.Collection(
                //    actual,
                //    x => Assert.Contains(expected[1].DeviceId, x.DeviceId)
                //);
                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Device>(expected[i], nameof(Device.UpdatedDt));
                    Assert.True(isEqual);
                }
            }


            [Fact, TestPriority(1)]
            public void Test_Device_FindBy_PlantId_LocationId()
            {
                string plantId = "JJ";
                string locationId = "TR01";
                using var repo = _fixture.DeviceFixture.CreateRepo();
                var expected = _fixture.Devices.Where(x => x.PlantId == plantId && x.LocationId == locationId)
                                               .OrderBy(x => x.DeviceId)
                                               .ToList();
                var actual = repo.FindBy(plantId, locationId).ToList();
                actual.ToList().ForEach(x =>
                {
                    _output.WriteLine($"id = {x.DeviceId}");
                    _output.WriteLine($"- Total Frames: {x.Frames.Count}");
                    _output.WriteLine($"- Total Rois: {x.Rois.Count}");
                    _output.WriteLine($"- Total Boxes: {x.Boxes.Count}");
                    _output.WriteLine($"- Total Cfgs: {x.Cfgs.Count}");
                });
                //Assert.Collection(
                //    actual,
                //    x => Assert.Contains(expected[1].DeviceId, x.DeviceId)
                //);
                for (int i = 0; i < expected.Count; i++)
                {
                    bool isEqual = actual[i].PublicPropertiesEqual<Device>(expected[i], nameof(Device.UpdatedDt));
                    Assert.True(isEqual);
                }
            }


            ///// <summary>
            /////
            ///// </summary>
            ///// <param name="json"></param>
            ///// <remarks>
            ///// https://stackoverflow.com/questions/60473360/how-to-implement-test-data-in-json-file-in-data-driven-unit-test-in-net-using-c
            ///// https://andrewlock.net/creating-parameterised-tests-in-xunit-with-inlinedata-classdata-and-memberdata/
            ///// </remarks>
            //[Theory, TestPriority(5)]
            //[MemberData(nameof(GetTemperatureTestData), parameters: 1)]
            //public void Test_Device_Add_TempData(TemperatureJson json)
            //{
            //    Device device = json.ToDevice();
            //    using (var repo = _fixture.CreateRepo())
            //    {
            //        var expected = device.DeviceId;
            //        repo.Add(device);
            //        var d = repo.Get(device.DeviceId);
            //        var actual = d.DeviceId;
            //        _output.WriteLine($"Total ROIs of {d.DeviceId}: {d.Rois.Count}");
            //        _output.WriteLine($"Total Boxes of {d.DeviceId}: {d.Boxes.Count}");

            //        Assert.Equal(expected, actual);
            //    }
            //}

            //public static IEnumerable<object[]> GetTemperatureTestData(int numTests)
            //{
            //    var data = new List<object[]>();
            //    var paths = new string[]
            //    {
            //    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DataSet", "20210630_145308-temp.json"),
            //    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DataSet", "20210630_180628-temp.json")
            //    };

            //    foreach (var path in paths)
            //    {
            //        using (var reader = new StreamReader(path, Encoding.UTF8))
            //        {
            //            var jsonString = reader.ReadToEnd();
            //            var json = JsonSerializer.Deserialize<TemperatureJson>(jsonString);

            //            data.Add(new object[] { json });
            //        }
            //    }

            //    return data.Take(numTests);
            //}

        }
    }

}
