using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class EvntLogRepository : GenericMapRepository<EvntLogEntity, EvntLog>, IEvntLogRepository, IDisposable
    {
        #region Constructors

        public EvntLogRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(int id)
        {
            return Context.Set<EvntLogEntity>().Any(x => x.Id == id);
        }

        public bool Exists(string deviceId, string ymd, string hms)
        {
            return Context.Set<EvntLogEntity>().Any(x => x.DeviceId == deviceId &&
                                                         x.Ymd == ymd &&
                                                         x.Hms == hms);
        }

        #region Get

        public EvntLog Get(int id)
        {
            return base.Get(id);
        }

        public async Task<EvntLog> GetAsync(int id)
        {
            return await base.GetAsync(id);
        }

        public List<EvntLog> GetLastest(string plantId, string deviceId = null, bool excludingInfoLevel = false)
        {
            List<EvntLog> results = new();

            // Check DB connection
            DbConnection conn = Context.Database.GetDbConnection();
            bool needClose = false;
            if (conn.State != ConnectionState.Open)
            {
                conn.Open();
                needClose = true;
            }

            try
            {
                using DbCommand cmd = Context.Database.GetDbConnection().CreateCommand();
                if (Context.Database.GetCommandTimeout().HasValue)
                    cmd.CommandTimeout = Context.Database.GetCommandTimeout().Value;

                Dictionary<string, string> names = new();

                if (String.IsNullOrEmpty(deviceId) == false)
                    names.Add("@device_id", deviceId);
                else
                    names.Add("@plant_id", plantId);

                List<DbParameter> parms = new();
                foreach (var name in names)
                {
                    DbParameter parm = cmd.CreateParameter();
                    parm.ParameterName = name.Key;
                    parm.Direction = ParameterDirection.Input;
                    parm.DbType = DbType.String;
                    parm.Value = name.Value;
                    parms.Add(parm);
                }

                string cmdText = String.Empty;

                if (String.IsNullOrEmpty(deviceId) == false)
                {
                    cmdText = excludingInfoLevel ?
                              @$"WITH stats AS (
                                  SELECT MAX(evnt_dt) AS max_evnt_dt
                                  FROM evnt_log
                                  WHERE device_id = @device_id
                               )
                               SELECT *
                               FROM evnt_log, stats
                               WHERE 1=1
                                  AND device_id = @device_id
                                  AND evnt_dt = stats.max_evnt_dt
                                  AND (evnt_lvl = '{EventLevel.Warning}' OR evnt_lvl = '{EventLevel.Urgent}' OR evnt_lvl = '{EventLevel.Error}')" :
                              @$"WITH stats AS(
                                  SELECT MAX(evnt_dt) AS max_evnt_dt
                                  FROM evnt_log
                                  WHERE device_id = @device_id
                               )
                              SELECT *
                              FROM evnt_log, stats
                              WHERE 1 = 1
                                  AND device_id = @device_id
                                  AND evnt_dt = stats.max_evnt_dt
                              ";
                }
                else
                {
                    cmdText = excludingInfoLevel ?
                              @$"WITH stats AS (
                                  SELECT MAX(evnt_dt) AS max_evnt_dt
                                  FROM evnt_log
                                  WHERE plant_id = @plant_id
                               )
                               SELECT *
                               FROM evnt_log, stats
                               WHERE 1=1
                                  AND plant_id = @plant_id
                                  AND evnt_dt = stats.max_evnt_dt
                                  AND (evnt_lvl = '{EventLevel.Warning}' OR evnt_lvl = '{EventLevel.Urgent}' OR evnt_lvl = '{EventLevel.Error}')" :
                              @$"WITH stats AS(
                                  SELECT MAX(evnt_dt) AS max_evnt_dt
                                  FROM evnt_log
                                  WHERE plant_id = @plant_id
                               )
                              SELECT *
                              FROM evnt_log, stats
                              WHERE 1 = 1
                                  AND plant_id = @plant_id
                                  AND evnt_dt = stats.max_evnt_dt
                              ";
                }

                cmd.CommandText = cmdText;
                cmd.CommandType = CommandType.Text;
                cmd.Parameters.AddRange(parms.ToArray());

                using IDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    EvntLog row = new();

                    int idx = reader.GetOrdinal("id");
                    if (reader.IsDBNull(idx) == false)
                        row.Id = (int)Convert.ChangeType(reader.GetValue(idx), typeof(int));

                    idx = reader.GetOrdinal("device_id");
                    if (reader.IsDBNull(idx) == false)
                        row.DeviceId = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("loc_id");
                    if (reader.IsDBNull(idx) == false)
                        row.LocationId = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("plant_id");
                    if (reader.IsDBNull(idx) == false)
                        row.PlantId = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("insp_area");
                    if (reader.IsDBNull(idx) == false)
                        row.InspArea = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("area_id");
                    if (reader.IsDBNull(idx) == false)
                        row.AreaId = (reader.GetValue(idx) == null) ? null : (int)Convert.ChangeType(reader.GetValue(idx), typeof(int));

                    idx = reader.GetOrdinal("ymd");
                    if (reader.IsDBNull(idx) == false)
                        row.Ymd = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("hms");
                    if (reader.IsDBNull(idx) == false)
                        row.Hms = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("evnt_dt");
                    if (reader.IsDBNull(idx) == false)
                        row.EvntDt = (DateTime)Convert.ChangeType(reader.GetValue(idx), typeof(DateTime));

                    idx = reader.GetOrdinal("evnt_type");
                    if (reader.IsDBNull(idx) == false)
                        row.EvntType = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("evnt_lvl");
                    if (reader.IsDBNull(idx) == false)
                        row.EvntLevel = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("diff_lvl");
                    if (reader.IsDBNull(idx) == false)
                        row.DiffLevel = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("set_value");
                    if (reader.IsDBNull(idx) == false)
                        row.SetValue = (reader.GetValue(idx) == null) ? null : (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("mea_value");
                    if (reader.IsDBNull(idx) == false)
                        row.MeaValue = (reader.GetValue(idx) == null) ? null : (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("title");
                    if (reader.IsDBNull(idx) == false)
                        row.Title = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("msg");
                    if (reader.IsDBNull(idx) == false)
                        row.Message = (string)Convert.ChangeType(reader.GetValue(idx), typeof(string));

                    idx = reader.GetOrdinal("emailed_dt");
                    if (reader.IsDBNull(idx) == false)
                        row.EmailedDt = (reader.GetValue(idx) == null) ? null : (DateTime)Convert.ChangeType(reader.GetValue(idx), typeof(DateTime));

                    idx = reader.GetOrdinal("upd_dt");
                    if (reader.IsDBNull(idx) == false)
                        row.UpdatedDt = (reader.GetValue(idx) == null) ? null : (DateTime)Convert.ChangeType(reader.GetValue(idx), typeof(DateTime));

                    results.Add(row);
                }
            }
            catch
            {
                throw;
            }
            finally
            {
                if (needClose)
                    conn.Close();
            }

            return results;
        }


        #endregion // Get

        #region FindBy

        public ICollection<EvntLog> FindBy(string deviceId)
        {
            var entities = Context.Set<EvntLogEntity>().Where(x => x.DeviceId == deviceId)
                                                       .OrderByDescending(x => x.Id);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<EvntLog>>(entities.ToList());
        }

        public ICollection<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            var entities = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                            String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                            x.DeviceId == deviceId);
            return entities.ToList();
        }

        public ICollection<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string evntLvl)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            var entities = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                            String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                            x.DeviceId == deviceId &&
                                            x.EvntLevel == evntLvl);
            return entities.ToList();
        }

        public ICollection<EvntLog> FindBy(DateTime startTime, DateTime endTime, string deviceId)
        {
            //var startYmdHms = startTime.ToYmdHms();
            //var endYmdHms = endTime.ToYmdHms();

            //var entities = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startYmdHms) >= 0 &&
            //                                String.Compare(String.Concat(x.Ymd, x.Hms), endYmdHms) <= 0 &&
            //                                x.DeviceId == deviceId);

           var entities = base.FindBy(x => x.EvntDt >= startTime &&
                                           x.EvntDt <= endTime &&
                                           x.DeviceId == deviceId);
            return entities.ToList();
        }

        public async Task<ICollection<EvntLog>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            return await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                              String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                              x.DeviceId == deviceId);
        }

        public async Task<ICollection<EvntLog>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId)
        {
            return await base.FindByAsync(x => x.EvntDt >= startTime &&
                                               x.EvntDt <= endTime &&
                                               x.DeviceId == deviceId);
        }

        public ICollection<EvntLog> FindByPlant(string plantId)
        {
            var entities = Context.Set<EvntLogEntity>().Where(x => x.Device.PlantId == plantId)
                                                       .OrderByDescending(x => x.Id);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<EvntLog>>(entities.ToList());
        }

        public ICollection<EvntLog> FindByPlant(string startYmd, string startHms, string endYmd, string endHms, string plantId)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            var entities = Context.Set<EvntLogEntity>().Where(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                                   String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                                   x.Device.PlantId == plantId)
                                                       .OrderByDescending(x => x.Id);

            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<EvntLog>>(entities.ToList());
        }


        #endregion // FindBy

        #region Create/Update/Delete

        public EvntLog Create(EvntLog evntLog)
        {
            bool exist = Exists(evntLog.Id);
            if (exist)
                return null;

            return base.Add(evntLog);
        }

        public async Task<EvntLog> CreateAsync(EvntLog evntLog)
        {
            bool exist = Exists(evntLog.Id);
            if (exist)
                return null;

            return await base.AddAsync(evntLog);
        }

        public EvntLog Update(EvntLog evntLog)
        {
            return base.Update(evntLog, evntLog.Id);
        }

        public async Task<EvntLog> UpdateAsync(EvntLog evntLog)
        {
            return await base.UpdateAsync(evntLog, evntLog.Id);
        }

        public int Delete(int id)
        {
            var found = Context.Set<EvntLogEntity>().Find(id);
            if (found == null)
                return 0;

            Context.Set<EvntLogEntity>().Remove(found);

            return Save();
        }

        public async Task<int> DeleteAsync(int id)
        {
            return await base.DeleteAsync(id);
        }

        #endregion // Create/Update/Delete

        #endregion // Public Methods

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
