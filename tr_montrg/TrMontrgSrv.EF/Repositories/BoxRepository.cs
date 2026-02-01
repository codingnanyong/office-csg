using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class BoxRepository : GenericMapRepository<BoxEntity, Box>, IBoxRepository, IDisposable
    {
        #region Fields


        #endregion

        #region Constructors

        public BoxRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        #region Get

        public Box Get(string ymd, string hms, string deviceId, int boxId)
        {
            return base.Get(ymd, hms, boxId, deviceId);
        }

        public async Task<Box> GetAsync(string ymd, string hms, string deviceId, int boxId)
        {
            return await base.GetAsync(ymd, hms, boxId, deviceId);
        }

        #endregion

        #region FindBy

        public ICollection<Box> FindBy(string ymd, string hms, string deviceId)
        {
            return FindBy(ymd, hms, ymd, hms, deviceId);
        }

        public async Task<ICollection<Box>> FindByAsync(string ymd, string hms, string deviceId)
        {
            return await FindByAsync(ymd, hms, ymd, hms, deviceId);
        }

        public ICollection<Box> FindBy(DateTime dt, string deviceId)
        {
            return FindBy(dt, dt, deviceId);
        }

        public async Task<ICollection<Box>> FindByAsync(DateTime dt, string deviceId)
        {
            return await FindByAsync(dt, dt, deviceId);
        }

        public ICollection<Box> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId = null)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            ICollection<Box> query = null;

            if (boxId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) == 0 &&
                                             x.DeviceId == deviceId &&
                                             x.BoxId == boxId.Value);
                }
                else
                {
                    query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                             String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                             x.DeviceId == deviceId &&
                                             x.BoxId == boxId.Value);
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

        public ICollection<Box> FindBy(DateTime startTime, DateTime endTime, string deviceId, int? boxId = null)
        {
            ICollection<Box> query = null;

            if (boxId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = base.FindBy(x => x.CaptureDt == startTime &&
                                             x.DeviceId == deviceId &&
                                             x.BoxId == boxId.Value);
                }
                else
                {
                    query = base.FindBy(x => x.CaptureDt >= startTime &&
                                             x.CaptureDt <= endTime &&
                                             x.DeviceId == deviceId &&
                                             x.BoxId == boxId.Value);
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

        public async Task<ICollection<Box>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId = null)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            ICollection<Box> query = null;

            if (boxId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) == 0 &&
                                                        String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                        x.DeviceId == deviceId &&
                                                        x.BoxId == boxId.Value);
                }
                else
                {
                    query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                        String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                        x.DeviceId == deviceId &&
                                                        x.BoxId == boxId.Value);
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
                                                        String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                        x.DeviceId == deviceId);
                }
            }

            return query;
        }

        public async Task<ICollection<Box>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, int? boxId = null)
        {
            ICollection<Box> query = null;

            if (boxId.HasValue)
            {
                if (startTime == endTime)
                {
                    query = await base.FindByAsync(x => x.CaptureDt == startTime &&
                                                        x.DeviceId == deviceId &&
                                                        x.BoxId == boxId.Value);
                }
                else
                {
                    query = await base.FindByAsync(x => x.CaptureDt >= startTime &&
                                                        x.CaptureDt <= endTime &&
                                                        x.DeviceId == deviceId &&
                                                        x.BoxId == boxId.Value);
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
            var found = Context.Set<BoxEntity>().Where(x => x.Ymd == ymd &&
                                                            x.Hms == hms &&
                                                            x.DeviceId == deviceId);
            if (found.Any() == false)
                return 0;

            Context.Set<BoxEntity>().RemoveRange(found);

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
