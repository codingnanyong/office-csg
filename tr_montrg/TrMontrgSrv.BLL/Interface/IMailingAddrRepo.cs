using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL.Interface
{
    /// <summary>
    ///
    /// </summary>
    public interface IMailingAddrRepo : IDisposable
    {
        bool Exists(string plantId, string email);

        MailingAddr Get(string plantId, string email);

        List<MailingAddr> FindAllActive();

        List<MailingAddr> FindAll(string plantId);

        MailingAddr Create(MailingAddr mailingAddr);

        Task<MailingAddr> CreateAsync(MailingAddr mailingAddr);

        MailingAddr Update(MailingAddr mailingAddr);

        Task<MailingAddr> UpdateAsync(MailingAddr mailingAddr);

        int Delete(string plantId, string email);

        Task<int> DeleteAsync(string plantId, string email);
    }
}
