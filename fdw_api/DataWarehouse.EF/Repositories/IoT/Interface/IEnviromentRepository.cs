using CSG.MI.DTO.IoT;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.IoT;
using Data;

namespace CSG.MI.FDW.EF.Repositories.IoT.Interface
{
    public interface IEnviromentRepository : IGenericMapRepository<TemperatureEntity, EnvironmentData , IoTDbContext>
    {
        DeviceInfo GetLocationEnvironment_Now(int loc);

        Task<DeviceInfo> GetLocationEnvironmentAsync_Now(int loc);

        DeviceInfo GetLocationEnvironment_Today(int loc);

        Task<DeviceInfo> GetLocationEnvironmentAsync_Today(int loc);

        DeviceInfo GetLocationEnvironment_Range(int loc, string start, string end = "");

        Task<DeviceInfo> GetLocationEnvironmentAsync_Range(int loc, string start, string end = "");
    }
}
