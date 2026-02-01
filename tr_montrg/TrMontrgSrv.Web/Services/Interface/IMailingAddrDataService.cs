using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IMailingAddrDataService : IDataService
    {
        Task<MailingAddr> Get(string plantId, string email);

        Task<List<MailingAddr>> GetList(string plantId);

        Task<List<MailingAddr>> GetActiveList();

        Task<MailingAddr> Create(MailingAddr mailingAddr);

        Task<bool> Update(MailingAddr mailingAddr);

        Task<bool> Delete(string group, string key);
    }
}
