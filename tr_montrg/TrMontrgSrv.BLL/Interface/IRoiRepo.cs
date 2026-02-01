using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;

namespace CSG.MI.TrMontrgSrv.BLL.Interface
{
    /// <summary>
    ///
    /// </summary>
    public interface IRoiRepo : IDisposable
    {
        Roi Get(string ymd, string hms, string deviceId, int roiId);

        Task<Roi> GetAsync(string ymd, string hms, string deviceId, int roiId);

        List<ImrData> GetImrChartData(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int roiId);

        Task<List<ImrData>> GetImrChartDataAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int roiId);

        ICollection<Roi> FindBy(string ymd, string hms, string deviceId);

        Task<ICollection<Roi>> FindByAsync(string ymd, string hms, string deviceId);

        ICollection<Roi> FindBy(DateTime dt, string deviceId);

        Task<ICollection<Roi>> FindByAsync(DateTime dt, string deviceId);

        List<Roi> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? roiId = null);

        List<Roi> FindBy(DateTime startTime, DateTime endTime, string deviceId, int? roiId = null);

        Task<ICollection<Roi>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? roiId = null);

        Task<ICollection<Roi>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, int? roiId = null);

        int Delete(string ymd, string hms, string deviceId);
    }
}
