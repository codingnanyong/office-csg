using CSG.MI.DTO.IoT;
using CSG.MI.FDW.BLL.IoT.Interface;
using CSG.MI.FDW.EF.Repositories.IoT.Interface;

namespace CSG.MI.FDW.BLL.IoT
{
    public class EnviromentRepo : IEnviromentRepo
    {
        #region Constructors

        private IEnviromentRepository _repo;

        public EnviromentRepo(IEnviromentRepository repo)
        {
            _repo = repo;
        }

        #endregion

        #region Public Methods

        public DeviceInfo GetLocationEnvironment_Now(int loc)
        {
            return _repo.GetLocationEnvironment_Now(loc);
        }

        public async Task<DeviceInfo> GetLocationEnvironmentAsync_Now(int loc)
        {
            return await _repo.GetLocationEnvironmentAsync_Now(loc);
        }

        public DeviceInfo GetLocationEnvironment_Today(int loc)
        {
            return _repo.GetLocationEnvironment_Today(loc);
        }

        public async Task<DeviceInfo> GetLocationEnvironmentAsync_Today(int loc)
        {
            return await _repo.GetLocationEnvironmentAsync_Today(loc);
        }

        public DeviceInfo GetLocationEnvironment_Range(int loc, string start, string end = "")
        {
            return _repo.GetLocationEnvironment_Range(loc,start, end);
        }

        public async Task<DeviceInfo> GetLocationEnvironmentAsync_Range(int loc, string start, string end = "")
        {
            return await _repo.GetLocationEnvironmentAsync_Range(loc,start, end);
        }

        #endregion

        #region Dispose

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_repo != null)
                {
                    _repo.Dispose();
                    _repo = null!;
                }
            }
        }
        #endregion
    }
}
