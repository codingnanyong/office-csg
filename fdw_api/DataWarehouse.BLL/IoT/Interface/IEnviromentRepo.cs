using CSG.MI.DTO.IoT;

namespace CSG.MI.FDW.BLL.IoT.Interface 
{
    public interface IEnviromentRepo : IDisposable
    {
        DeviceInfo GetLocationEnvironment_Now(int loc);

        Task<DeviceInfo> GetLocationEnvironmentAsync_Now(int loc);

        DeviceInfo GetLocationEnvironment_Today(int loc);

        Task<DeviceInfo> GetLocationEnvironmentAsync_Today(int loc);

        DeviceInfo GetLocationEnvironment_Range(int loc, string start, string end = "");

        Task<DeviceInfo> GetLocationEnvironmentAsync_Range(int loc, string start, string end = "");
    }
}
