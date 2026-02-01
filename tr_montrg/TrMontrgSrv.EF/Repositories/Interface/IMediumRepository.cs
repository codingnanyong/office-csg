using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IMediumRepository : IGenericMapRepository<MediumEntity, Medium>
    {
        Medium Get(string ymd, string hms, string deviceId, string mediumType);

        Task<Medium> GetAsync(string ymd, string hms, string deviceId, string mediumType);

        ICollection<Medium> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string mediumType = null);

        ICollection<Medium> FindBy(DateTime startTime, DateTime endTime, string deviceId, string mediumType = null);

        Task<ICollection<Medium>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string mediumType = null);

        Task<ICollection<Medium>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, string mediumType = null);

        int Delete(string ymd, string hms, string deviceId);
    }
}
