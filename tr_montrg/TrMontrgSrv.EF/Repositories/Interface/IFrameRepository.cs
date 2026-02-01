using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model;
using CSG.MI.TrMontrgSrv.Model.ApiData;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.Interface
{
    public interface IFrameRepository : IGenericMapRepository<FrameEntity, Frame>
    {
        Frame Get(string ymd, string hms, string deviceId);

        Task<Frame> GetAsync(string ymd, string hms, string deviceId);

        List<ImrData> GetImrChartData(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        Task<List<ImrData>> GetImrChartDataAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        ICollection<Frame> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        ICollection<Frame> FindBy(DateTime startTime, DateTime endTime, string deviceId);

        Task<ICollection<Frame>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        Task<ICollection<Frame>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId);

        int Delete(string ymd, string hms, string deviceId);
    }
}
