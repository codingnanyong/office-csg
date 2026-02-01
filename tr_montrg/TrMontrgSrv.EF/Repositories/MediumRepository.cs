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
    public class MediumRepository : GenericMapRepository<MediumEntity, Medium>, IMediumRepository, IDisposable
    {
        #region Constructors

        public MediumRepository(AppDbContext ctx) : base(ctx)
        {

        }

        #endregion

        #region Public Methods

        #region Get

        public Medium Get(string ymd, string hms, string deviceId, string mediumType)
        {
            return base.Get(ymd, hms, mediumType, deviceId);
        }

        public async Task<Medium> GetAsync(string ymd, string hms, string deviceId, string mediumType)
        {
            return await base.GetAsync(ymd, hms, mediumType, deviceId);
        }

        #endregion

        #region FindBy

        public ICollection<Medium> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string mediumType = null)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            ICollection<Medium> query = null;

            if (mediumType == null)
            {
                query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                         String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                         x.DeviceId == deviceId);
            }
            else
            {
                query = base.FindBy(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                         String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                         x.DeviceId == deviceId &&
                                         x.MediumType == mediumType);
            }

            return query.ToList();
        }

        public ICollection<Medium> FindBy(DateTime startTime, DateTime endTime, string deviceId, string mediumType = null)
        {
            ICollection<Medium> query = null;

            if (mediumType == null)
            {
                query = base.FindBy(x => x.CaptureDt >= startTime &&
                                         x.CaptureDt <= endTime &&
                                         x.DeviceId == deviceId);
            }
            else
            {
                query = base.FindBy(x => x.CaptureDt >= startTime &&
                                         x.CaptureDt <= endTime &&
                                         x.DeviceId == deviceId &&
                                         x.MediumType == mediumType);
            }

            return query.ToList();
        }

        public async Task<ICollection<Medium>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string mediumType = null)
        {
            string startTime = String.Concat(startYmd, startHms);
            string endTime = String.Concat(endYmd, endHms);

            ICollection<Medium> query = null;

            if (mediumType == null)
            {
                query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                    String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                    x.DeviceId == deviceId);
            }
            else
            {
                query = await base.FindByAsync(x => String.Compare(String.Concat(x.Ymd, x.Hms), startTime) >= 0 &&
                                                    String.Compare(String.Concat(x.Ymd, x.Hms), endTime) <= 0 &&
                                                    x.DeviceId == deviceId &&
                                                    x.MediumType == mediumType);
            }


            return query;
        }

        public async Task<ICollection<Medium>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, string mediumType = null)
        {
            ICollection<Medium> query = null;

            if (mediumType == null)
            {
                query = await base.FindByAsync(x => x.CaptureDt >= startTime &&
                                                    x.CaptureDt <= endTime &&
                                                    x.DeviceId == deviceId);
            }
            else
            {
                query = await base.FindByAsync(x => x.CaptureDt >= startTime &&
                                                    x.CaptureDt <= endTime &&
                                                    x.DeviceId == deviceId &&
                                                    x.MediumType == mediumType);
            }

            return query;
        }

        #endregion

        #region Delete

        public int Delete(string ymd, string hms, string deviceId)
        {
            var found = Context.Set<MediumEntity>().Where(x => x.Ymd == ymd &&
                                                               x.Hms == hms &&
                                                               x.DeviceId == deviceId);
            if (found.Any() == false)
                return 0;

            Context.Set<MediumEntity>().RemoveRange(found);

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
