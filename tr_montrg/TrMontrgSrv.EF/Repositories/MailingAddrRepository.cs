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
    public class MailingAddrRepository : GenericMapRepository<MailingAddrEntity, MailingAddr>, IMailingAddrRepository, IDisposable
    {
        #region Constructors

        public MailingAddrRepository(AppDbContext ctx) : base(ctx)
        {
        }

        #endregion

        #region Public Methods

        public bool Exists(string plantId, string email)
        {
            return Context.Set<MailingAddrEntity>().Any(x => x.PlantId == plantId && x.Email == email);
        }

        public MailingAddr Get(string plantId, string email)
        {
            var entity = Context.Set<MailingAddrEntity>()
                            .Where(x => x.PlantId == plantId && x.Email == email)
                            .FirstOrDefault();
            return Context.Mapper.Map<MailingAddr>(entity);
        }

        public ICollection<MailingAddr> FindAllActive()
        {
            var entities = Context.Set<MailingAddrEntity>().Where(x => x.Inactive == false);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<MailingAddr>>(entities.ToList());
        }

        public ICollection<MailingAddr> FindAll(string plantId)
        {
            var entities = Context.Set<MailingAddrEntity>().Where(x => x.PlantId == plantId);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<MailingAddr>>(entities.ToList());
        }

        public new ICollection<MailingAddr> FindAll(Expression<Func<MailingAddrEntity, bool>> predicate)
        {
            var entities = Context.Set<MailingAddrEntity>().Where(predicate);
            Debug.WriteLine(entities.ToQueryString());

            return Context.Mapper.Map<ICollection<MailingAddr>>(entities.ToList());
        }

        public MailingAddr Create(MailingAddr mailingAddr)
        {
            bool exist = Exists(mailingAddr.PlantId, mailingAddr.Email);
            if (exist)
                return null;

            return base.Add(mailingAddr);
        }

        public async Task<MailingAddr> CreateAsync(MailingAddr mailingAddr)
        {
            bool exist = Exists(mailingAddr.PlantId, mailingAddr.Email);
            if (exist)
                return null;

            return await base.AddAsync(mailingAddr);
        }

        public MailingAddr Update(MailingAddr mailingAddr)
        {
            return base.Update(mailingAddr, mailingAddr.PlantId, mailingAddr.Email);
        }

        public async Task<MailingAddr> UpdateAsync(MailingAddr mailingAddr)
        {
            return await base.UpdateAsync(mailingAddr, mailingAddr.PlantId, mailingAddr.Email);
        }

        public int Delete(string plantId, string email)
        {
            return base.Delete(plantId, email);
        }

        public async Task<int> DeleteAsync(string plantId, string email)
        {
            return await base.DeleteAsync(plantId, email);
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
