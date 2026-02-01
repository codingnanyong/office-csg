using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Repositories
{
    public class GrpKeyRepository : GenericMapRepository<GrpKeyEntity, GrpKey>, IGrpKeyRepository, IDisposable
    {
        #region Constructors

        public GrpKeyRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(string group, string key)
        {
            return Context.Set<GrpKeyEntity>().Any(x => x.Group == group && x.Key == key);
        }

        public GrpKey Get(string group, string key)
        {
            var entity = Context.Set<GrpKeyEntity>()
                            .Where(x => x.Group == group && x.Key == key)
                            .FirstOrDefault();
            return Context.Mapper.Map<GrpKey>(entity);
        }

        public ICollection<GrpKey> FindAll(string group)
        {
            var entities = Context.Set<GrpKeyEntity>().Where(x => x.Group == group)
                                                      .OrderBy(x => x.Order);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<GrpKey>>(entities.ToList());
        }

        public new ICollection<GrpKey> FindAll(Expression<Func<GrpKeyEntity, bool>> predicate)
        {
            var entities = Context.Set<GrpKeyEntity>().Where(predicate);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<GrpKey>>(entities.ToList());
        }

        public GrpKey Create(GrpKey grpKey)
        {
            bool exist = Exists(grpKey.Group, grpKey.Key);
            if (exist)
                return null;

            return base.Add(grpKey);
        }

        public async Task<GrpKey> CreateAsync(GrpKey grpKey)
        {
            bool exist = Exists(grpKey.Group, grpKey.Key);
            if (exist)
                return null;

            return await base.AddAsync(grpKey);
        }

        public GrpKey Update(GrpKey grpKey)
        {
            return base.Update(grpKey, grpKey.Group, grpKey.Key);
        }

        public async Task<GrpKey> UpdateAsync(GrpKey grpKey)
        {
            return await base.UpdateAsync(grpKey, grpKey.Group, grpKey.Key);
        }

        public int Delete(string group, string key)
        {
            return base.Delete(group, key);
        }

        public async Task<int> DeleteAsync(string group, string key)
        {
            return await base.DeleteAsync(group, key);
        }

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
