using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class RoiRepository : GenericMapRepository<RoiEntity, Roi>, IRoiRepository, IDisposable
    {
        #region Fields


        #endregion

        #region Constructors

        public RoiRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        #region Get

        public Roi Get(string ymd, string hms, string deviceId, int roiId)
        {
            return base.Get(ymd, hms, roiId, deviceId);
        }

        public async Task<Roi> GetAsync(string ymd, string hms, string deviceId, int roiId)
        {
            return await base.GetAsync(ymd, hms, roiId, deviceId);
        }

        public List<ImrData> GetImrChartData(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int roiId)
        {
            List<ImrData> results = new();

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

                List<DbParameter> parms = new();

                DbParameter deviceIdParm = cmd.CreateParameter();
                deviceIdParm.ParameterName = "@device_id";
                deviceIdParm.Direction = ParameterDirection.Input;
                deviceIdParm.DbType = DbType.String;
                deviceIdParm.Value = deviceId;
                parms.Add(deviceIdParm);

                DbParameter roiIdParm = cmd.CreateParameter();
                roiIdParm.ParameterName = "@roi_id";
                roiIdParm.Direction = ParameterDirection.Input;
                roiIdParm.DbType = DbType.Int32;
                roiIdParm.Value = roiId;
                parms.Add(roiIdParm);

                Dictionary<string, string> names = new()
                {
                    { "@start_ymd", startYmd },
                    { "@start_hms", startHms },
                    { "@end_ymd", endYmd },
                    { "@end_hms", endHms }
                };
                foreach (var name in names)
                {
                    DbParameter parm = cmd.CreateParameter();
                    parm.ParameterName = name.Key;
                    parm.Direction = ParameterDirection.Input;
                    parm.DbType = DbType.String;
                    parm.Value = name.Value;
                    parms.Add(parm);
                }

                cmd.CommandText = "fn_roi_imr_data";
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.Parameters.AddRange(parms.ToArray());

                using IDataReader reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    ImrData row = new();

                    int idx = reader.GetOrdinal("dt");
                    if (reader.IsDBNull(idx) == false)
                        row.Dt = (DateTime)Convert.ChangeType(reader.GetValue(idx), typeof(DateTime));

                    idx = reader.GetOrdinal("tmax");
                    if (reader.IsDBNull(idx) == false)
                        row.TMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("tmin");
                    if (reader.IsDBNull(idx) == false)
                        row.TMin = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("tmin_frame");
                    if (reader.IsDBNull(idx) == false)
                        row.TMinFrame = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("diff");
                    if (reader.IsDBNull(idx) == false)
                        row.Diff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("mr_max");
                    if (reader.IsDBNull(idx) == false)
                        row.MrMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("mr_sign_max");
                    if (reader.IsDBNull(idx) == false)
                        row.MrSignMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("xbar_max");
                    if (reader.IsDBNull(idx) == false)
                        row.XBarMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("mr_bar_max");
                    if (reader.IsDBNull(idx) == false)
                        row.MrBarMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("ucl_i_max");
                    if (reader.IsDBNull(idx) == false)
                        row.UclIMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("lcl_i_max");
                    if (reader.IsDBNull(idx) == false)
                        row.LclIMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("ucl_mr_max");
                    if (reader.IsDBNull(idx) == false)
                        row.UclMrMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("lcl_mr_max");
                    if (reader.IsDBNull(idx) == false)
                        row.LclMrMax = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("mr_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.MrDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("mr_sign_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.MrSignDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("xbar_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.XBarDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("mr_bar_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.MrBarDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("ucl_i_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.UclIDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("lcl_i_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.LclIDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("ucl_mr_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.UclMrDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

                    idx = reader.GetOrdinal("lcl_mr_diff");
                    if (reader.IsDBNull(idx) == false)
                        row.LclMrDiff = (float)Convert.ChangeType(reader.GetValue(idx), typeof(float));

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

        public async Task<List<ImrData>> GetImrChartDataAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int roiId)
        {
            return await Task.Run(() => GetImrChartData(startYmd, startHms, endYmd, endHms, deviceId, roiId));
        }

        #endregion

        #region FindBy

        public ICollection<Roi> FindBy(string ymd, string hms, string deviceId)
        {
            return FindBy(ymd, hms, ymd, hms, deviceId);
        }

        public async Task<ICollection<Roi>> FindByAsync(string ymd, string hms, string deviceId)
        {
            return await FindByAsync(ymd, hms, ymd, hms, deviceId);
        }

        public ICollection<Roi> FindBy(DateTime dt, string deviceId)
        {
            return FindBy(dt, dt, deviceId);
        }

        public async Task<ICollection<Roi>> FindByAsync(DateTime dt, string deviceId)
        {
            return await FindByAsync(dt, dt, deviceId);
        }

        public ICollection<Roi> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? roiId = null)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            ICollection<Roi> query = null;

            if (roiId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) == 0 &&
                                             x.DeviceId == deviceId &&
                                             x.RoiId == roiId.Value);
                }
                else
                {
                    query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                             String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                             x.DeviceId == deviceId &&
                                             x.RoiId == roiId.Value);
                }
            }
            else
            {
                if (startTime == endTime)
                {
                    query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) == 0 &&
                                             x.DeviceId == deviceId);
                }
                else
                {
                    query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                             String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                             x.DeviceId == deviceId);
                }

            }

            return query.ToList();
        }

        public ICollection<Roi> FindBy(DateTime startTime, DateTime endTime, string deviceId, int? roiId = null)
        {
            ICollection<Roi> query = null;

            if (roiId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = base.FindBy(x => x.CaptureDt == startTime &&
                                             x.DeviceId == deviceId &&
                                             x.RoiId == roiId.Value);
                }
                else
                {
                    query = base.FindBy(x => x.CaptureDt >= startTime &&
                                             x.CaptureDt <= endTime &&
                                             x.DeviceId == deviceId &&
                                             x.RoiId == roiId.Value);
                }


            }
            else
            {
                if (startTime == endTime)
                {
                    query = base.FindBy(x => x.CaptureDt == startTime &&
                                             x.DeviceId == deviceId);
                }
                else
                {
                    query = base.FindBy(x => x.CaptureDt >= startTime &&
                                             x.CaptureDt <= endTime &&
                                             x.DeviceId == deviceId);
                }
            }

            return query.ToList();
        }

        public async Task<ICollection<Roi>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? roiId = null)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            ICollection<Roi> query = null;

            if (roiId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) == 0 &&
                                                        x.DeviceId == deviceId &&
                                                        x.RoiId == roiId.Value);
                }
                else
                {
                    query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                        String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                        x.DeviceId == deviceId &&
                                                        x.RoiId == roiId.Value);
                }
            }
            else
            {
                if (startTime == endTime)
                {
                    query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) == 0 &&
                                                        x.DeviceId == deviceId);
                }
                else
                {
                    query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                        String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 && x.DeviceId == deviceId);
                }

            }

            return query;
        }

        public async Task<ICollection<Roi>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, int? roiId = null)
        {
            ICollection<Roi> query = null;

            if (roiId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = await base.FindByAsync(x => x.CaptureDt == startTime &&
                                                        x.DeviceId == deviceId &&
                                                        x.RoiId == roiId.Value);
                }
                else
                {
                    query = await base.FindByAsync(x => x.CaptureDt >= startTime &&
                                                        x.CaptureDt <= endTime &&
                                                        x.DeviceId == deviceId &&
                                                        x.RoiId == roiId.Value);
                }
            }
            else
            {
                if (startTime == endTime)
                {
                    query = await base.FindByAsync(x => x.CaptureDt == startTime &&
                                                        x.DeviceId == deviceId);
                }
                else
                {
                    query = await base.FindByAsync(x => x.CaptureDt >= startTime &&
                                                        x.CaptureDt <= endTime &&
                                                        x.DeviceId == deviceId);
                }
            }

            return query;
        }

        #endregion

        #region Delete

        public int Delete(string ymd, string hms, string deviceId)
        {
            var found = Context.Set<RoiEntity>().Where(x => x.Ymd == ymd &&
                                                             x.Hms == hms &&
                                                             x.DeviceId == deviceId);
            if (found.Any() == false)
                return 0;

            Context.Set<RoiEntity>().RemoveRange(found);

            //return found.Count();
            return Save();
        }

        #endregion

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
