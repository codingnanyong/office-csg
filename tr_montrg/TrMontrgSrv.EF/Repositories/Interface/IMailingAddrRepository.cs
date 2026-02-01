using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IMailingAddrRepository : IGenericMapRepository<MailingAddrEntity, MailingAddr>
    {
        bool Exists(string plantId, string email);

        MailingAddr Get(string plantId, string email);

        ICollection<MailingAddr> FindAllActive();

        ICollection<MailingAddr> FindAll(string plantId);

        new ICollection<MailingAddr> FindAll(Expression<Func<MailingAddrEntity, bool>> predicate);

        MailingAddr Create(MailingAddr mailingAddr);

        Task<MailingAddr> CreateAsync(MailingAddr mailingAddr);

        MailingAddr Update(MailingAddr mailingAddr);

        Task<MailingAddr> UpdateAsync(MailingAddr mailingAddr);

        int Delete(string plantId, string email);

        Task<int> DeleteAsync(string plantId, string email);
    }
}