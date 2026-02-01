using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.BLL.Interface;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.EF.Repositories.Interface;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL
{
    /// <summary>
    ///
    /// </summary>
    public class MailingAddrRepo : IMailingAddrRepo
    {
        private IMailingAddrRepository _repo;

        public MailingAddrRepo(IMailingAddrRepository repo)
        {
            _repo = repo;
        }

        public bool Exists(string plantId, string email)
        {
            if (String.IsNullOrWhiteSpace(plantId) || String.IsNullOrWhiteSpace(email))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Exists(plantId, email);
        }

        public MailingAddr Get(string plantId, string email)
        {
            if (String.IsNullOrWhiteSpace(plantId) || String.IsNullOrWhiteSpace(email))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.Get(plantId, email);
        }

        public List<MailingAddr> FindAllActive()
        {
            return _repo.FindAllActive().ToList();
        }

        public List<MailingAddr> FindAll(string plantId)
        {
            if (String.IsNullOrWhiteSpace(plantId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            return _repo.FindAll(plantId).ToList();
        }

        public MailingAddr Create(MailingAddr mailingAddr)
        {
            return _repo.Create(mailingAddr);
        }

        public async Task<MailingAddr> CreateAsync(MailingAddr mailingAddr)
        {
            return await _repo.CreateAsync(mailingAddr);
        }

        public MailingAddr Update(MailingAddr mailingAddr)
        {
            return _repo.Update(mailingAddr);
        }

        public async Task<MailingAddr> UpdateAsync(MailingAddr mailingAddr)
        {
            return await _repo.UpdateAsync(mailingAddr);
        }

        public int Delete(string plantId, string email)
        {
            return _repo.Delete(plantId, email);
        }

        public async Task<int> DeleteAsync(string plantId, string email)
        {
            return await _repo.DeleteAsync(plantId, email);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_repo != null)
                {
                    _repo.Dispose();
                    _repo = null;
                }
            }
        }
    }
}
