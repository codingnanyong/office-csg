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
using CSG.MI.TrMontrgSrv.Model.ApiData;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class FrameRepository : GenericMapRepository<FrameEntity, Frame>, IFrameRepository, IDisposable
    {
        #region Fields


        #endregion

        #region Constructors

        public FrameRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        #region Get

        public Frame Get(string ymd, string hms, string deviceId)
        {
            return base.Get(ymd, hms, deviceId);
        }

        public async Task<Frame> GetAsync(string ymd, string hms, string deviceId)
        {
            return await base.GetAsync(ymd, hms, deviceId);
        }

        public List<ImrData> GetImrChartData(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
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

                Dictionary<string, string> names = new()
                {
                    { "@device_id", deviceId },
                    { "@start_ymd", startYmd },
                    { "@start_hms", startHms },
                    { "@end_ymd", endYmd },
                    { "@end_hms", endHms }
                };

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

                cmd.CommandText = "fn_frame_imr_data";
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

                    row.TMinFrame = row.TMin;

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

        public async Task<List<ImrData>> GetImrChartDataAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            return await Task.Run(() => GetImrChartData(startYmd, startHms, endYmd, endHms, deviceId));
        }

        #endregion // Get

        #region FindBy

        public ICollection<Frame> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            var entities = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                            String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                            x.DeviceId == deviceId);

            return entities.ToList();
        }

        public ICollection<Frame> FindBy(DateTime startTime, DateTime endTime, string deviceId)
        {
            //var startYmdHms = startTime.ToYmdHms();
            //var endYmdHms = endTime.ToYmdHms();

            //var entities = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startYmdHms) >= 0 &&
            //                                String.Compare(String.Concat(x.Ymd, x.Hms), endYmdHms) <= 0 &&
            //                                x.DeviceId == deviceId);

           var entities = base.FindBy(x => x.CaptureDt >= startTime &&
                                           x.CaptureDt <= endTime &&
                                           x.DeviceId == deviceId);
            return entities;
        }

        public async Task<ICollection<Frame>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            return await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                              String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                              x.DeviceId == deviceId);
        }

        public async Task<ICollection<Frame>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId)
        {
            return await base.FindByAsync(x => x.CaptureDt >= startTime &&
                                               x.CaptureDt <= endTime &&
                                               x.DeviceId == deviceId);
        }

        #endregion // FindBy

        #region Delete

        public int Delete(string ymd, string hms, string deviceId)
        {
            var found = Context.Set<FrameEntity>().Find(ymd, hms, deviceId);
            if (found == null)
                return 0;

            Context.Set<FrameEntity>().Remove(found);

            return Save();
        }

        #endregion // Delete

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
