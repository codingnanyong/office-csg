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
    public interface IMediumRepo : IDisposable
    {
        Medium Get(string ymd, string hms, string deviceId, MediumType mediumType);

        Task<Medium> GetAsync(string ymd, string hms, string deviceId, MediumType mediumType);

        List<Medium> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, MediumType? mediumType = null);

        List<Medium> FindBy(DateTime startTime, DateTime endTime, string deviceId, MediumType? mediumType = null);

        Task<ICollection<Medium>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, MediumType? mediumType = null);

        Task<ICollection<Medium>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, MediumType? mediumType = null);
    }
}
