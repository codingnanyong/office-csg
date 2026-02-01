using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AutoMapper.QueryableExtensions;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class CfgRepository : GenericMapRepository<CfgEntity, Cfg>, ICfgRepository, IDisposable
    {
        #region Fields


        #endregion

        #region Constructors

        public CfgRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public Cfg Get(int id)
        {
            return base.Get(id);
        }

        public async Task<Cfg> GetAsync(int id)
        {
            return await base.GetAsync(id);
        }

        public Cfg GetLast(string deviceId)
        {
            var entity = _context.CfgDbSet.Where(x => x.DeviceId == deviceId)
                                          .OrderByDescending(x => x.CaptureDt)
                                          .FirstOrDefault();

            return _context.Mapper.Map<Cfg>(entity);
        }

        public ICollection<Cfg> GetLast(string deviceId, int last)
        {
            var query = _context.CfgDbSet.Where(x => x.DeviceId == deviceId)
                                         .OrderByDescending(x => x.CaptureDt)
                                         .Take(last);

            Debug.WriteLine(query.ToQueryString());

            return query.ProjectTo<Cfg>(_context.Mapper.ConfigurationProvider).ToList();
        }

        public ICollection<Cfg> FindBy(string deviceId)
        {
            var query = base.FindBy(x => x.DeviceId == deviceId);
            return query.ToList();
        }

        public async Task<ICollection<Cfg>> FindByAsync(string deviceId)
        {
            return await base.FindAllAsync(x => x.DeviceId == deviceId);
        }

        #endregion Public Methods

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
