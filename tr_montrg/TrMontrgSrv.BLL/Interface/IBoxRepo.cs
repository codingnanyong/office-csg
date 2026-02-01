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
    public interface IBoxRepo : IDisposable
    {
        Box Get(string ymd, string hms, string deviceId, int boxId);

        Task<Box> GetAsync(string ymd, string hms, string deviceId, int boxId);

        ICollection<Box> FindBy(string ymd, string hms, string deviceId);

        Task<ICollection<Box>> FindByAsync(string ymd, string hms, string deviceId);

        ICollection<Box> FindBy(DateTime dt, string deviceId);

        Task<ICollection<Box>> FindByAsync(DateTime dt, string deviceId);

        List<Box> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId = null);

        List<Box> FindBy(DateTime startTime, DateTime endTime, string deviceId, int? boxId = null);

        Task<ICollection<Box>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId = null);

        Task<ICollection<Box>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, int? boxId = null);

        int Delete(string ymd, string hms, string deviceId);
    }
}
