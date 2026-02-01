using CSG.MI.DTO.IoT;
using CSG.MI.FDW.EF.Core.Repo;
using CSG.MI.FDW.EF.Entities.IoT;
using CSG.MI.FDW.EF.Repositories.IoT.Interface;
using Data;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using System.Globalization;

namespace CSG.MI.FDW.EF.Repositories.IoT
{
    public class EnviromentRepository : GenericMapRepository<TemperatureEntity, EnvironmentData, IoTDbContext>, IEnviromentRepository, IDisposable
    {
        #region Constructors

        public EnviromentRepository(IoTDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public DeviceInfo GetLocationEnvironment_Now(int loc)
        {
            return GetLatestEnvironmentDataByLocation(loc);
        }

        public Task<DeviceInfo> GetLocationEnvironmentAsync_Now(int loc)
        {
            return Task.Run(() => GetLatestEnvironmentDataByLocation(loc));
        }

        public DeviceInfo GetLocationEnvironment_Today(int loc)
        {
            return GetTodayEnvironmentDataByLocation(loc);
        }

        public Task<DeviceInfo> GetLocationEnvironmentAsync_Today(int loc)
        {
            return Task.Run(() => GetTodayEnvironmentDataByLocation(loc));
        }

        public DeviceInfo GetLocationEnvironment_Range(int loc,string start, string end = "")
        {
            return GetRangeEnvironmentDataByLocation(loc,start, end);
        }

        public Task<DeviceInfo> GetLocationEnvironmentAsync_Range(int loc, string start, string end = "")
        {
            return Task.Run(() => GetRangeEnvironmentDataByLocation(loc,start, end));
        }

        #endregion

        #region Private Methods

        private DeviceInfo GetLatestEnvironmentDataByLocation(int loc)
        {
            var location = GetLocationName(loc);

            var now = DateTime.Now.Date;

            var sql = @"SELECT t.ymd, t.hmsf, t.sensor_id, t.device_id, t.capture_dt, t.t1, t.t2
                        FROM (SELECT subt.ymd, subt.hmsf, subt.capture_dt, subt.sensor_id, subt.device_id, subt.t1, subt.t2,
                                   ROW_NUMBER() OVER (PARTITION BY subt.sensor_id ORDER BY subt.capture_dt DESC) AS rn
                              FROM workshop.temperature AS subt
                              WHERE subt.capture_dt >= @Now) AS t
                        WHERE rn = 1
                        ORDER BY t.capture_dt DESC";
            try
            {
                var result = _context.temperature.FromSqlRaw(sql, new NpgsqlParameter("Now", now))
                                                 .ToList();

                var env = TransformToDeviceInfo(result, location);

                return env ?? null!;
            }
            catch (Exception ex)
            {
                throw new ApplicationException("Error occurred while fetching environment data for now.", ex);
            }
        }

        private DeviceInfo GetTodayEnvironmentDataByLocation(int loc)
        {
            var today = DateTime.Today;
            var location = GetLocationName(loc);

            var sql = @"SELECT DISTINCT ON (sensor_id, DATE_TRUNC('hour', capture_dt), FLOOR(EXTRACT(MINUTE FROM capture_dt) / 30))sensor_id, capture_dt, ymd, hmsf, device_id, t1, t2
                        FROM workshop.temperature
                        WHERE DATE(capture_dt) = @Today
                        ORDER BY sensor_id, DATE_TRUNC('hour', capture_dt), FLOOR(EXTRACT(MINUTE FROM capture_dt) / 30), capture_dt";
            try
            {
                var result = _context.temperature.FromSqlRaw(sql, new NpgsqlParameter("Today", today))
                                                 .ToList();

                var env = TransformToDeviceInfo(result, location);

                return env ?? null!;

            }
            catch (Exception ex)
            {
                throw new ApplicationException("Error occurred while fetching environment data for now.", ex);
            }
        }

        private DeviceInfo GetRangeEnvironmentDataByLocation(int loc, string start, string end = "")
        {
            DateTime startDate;
            DateTime endDate;

            if (!DateTime.TryParseExact(start, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out startDate))
            {
                throw new ArgumentException("Invalid start date format. Please use yyyyMMdd format.");
            }

            startDate = startDate.Date;

            if (string.IsNullOrEmpty(end))
            {
                endDate = DateTime.Now;
            }
            else if (!DateTime.TryParseExact(end, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out endDate))
            {
                throw new ArgumentException("Invalid end date format. Please use yyyyMMdd format.");
            }

            if (startDate > endDate)
            {
                throw new ArgumentException("Start date cannot be after end date.");
            }

            var location = GetLocationName(loc);

            var sql = @"SELECT DISTINCT ON (sensor_id, DATE_TRUNC('hour', capture_dt), FLOOR(EXTRACT(MINUTE FROM capture_dt) / 30))
                                            sensor_id, capture_dt, ymd, hmsf, device_id, t1, t2
                        FROM workshop.temperature
                        WHERE ymd BETWEEN @Start AND @End
                        ORDER BY sensor_id, DATE_TRUNC('hour', capture_dt), FLOOR(EXTRACT(MINUTE FROM capture_dt) / 30), capture_dt";

            try
            {
                var result = _context.temperature.FromSqlRaw(sql, new NpgsqlParameter("Start", NpgsqlDbType.Text) { Value = startDate.ToString("yyyyMMdd") },
                                                                  new NpgsqlParameter("End", NpgsqlDbType.Text) { Value = endDate.ToString("yyyyMMdd") })
                                                 .AsEnumerable().ToList();

                var env = TransformToDeviceInfo(result, location);

                return env ?? null!;

            }
            catch (Exception ex)
            {
                throw new ApplicationException("Error occurred while fetching environment data for the specified date range.", ex);
            }
        }

        private string GetLocationName(int loc)
        {
            return loc switch
            {
                1 => "3D Printer Room",
                2 => "나염실",
                _ => throw new ArgumentException("Invalid location code.")
            };
        }

        private DeviceInfo TransformToDeviceInfo(IEnumerable<TemperatureEntity> result, string location)
        {
            var sensors = result.Join(_context.sensors.ToList(), rst => rst.SensorId, mst => mst.SensorId, (rst, mst) => new { rst, mst })
                                .Where(jnd => jnd.mst.MachId == location)
                                .GroupBy(jnd => jnd.rst.SensorId)
                                .Select(snsr => new SensorInfo
                                {
                                    SensorId = snsr.Key,
                                    Environments = snsr.Select(env => new EnvironmentData
                                    {
                                        MeasureTime = env.rst.CaptureDate.AddHours(9),
                                        Temperature = env.rst.Temperature,
                                        Humidity = env.rst.Humidity
                                    }).ToList()
                                }).ToList();

            return new DeviceInfo
            {
                Location = location,
                Sensors = sensors
            };
        }


        #endregion

        #region Disposable

        protected override void Dispose(bool disposing)
        {
            if (IsDisposed == false)
            {
                if (disposing)
                {
                    // Dispose managed objects.
                }

                // Free unmanaged resources and override a finalizer below.
                // Set large fields to null.
            }

            base.Dispose(disposing);
        }

        #endregion
    }
}
