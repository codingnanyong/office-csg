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
    public interface IBoxRepository : IGenericMapRepository<BoxEntity, Box>
    {
        Box Get(string ymd, string hms, string deviceId, int boxId);

        Task<Box> GetAsync(string ymd, string hms, string deviceId, int boxId);

        ICollection<Box> FindBy(string ymd, string hms, string deviceId);

        Task<ICollection<Box>> FindByAsync(string ymd, string hms, string deviceId);

        ICollection<Box> FindBy(DateTime dt, string deviceId);

        Task<ICollection<Box>> FindByAsync(DateTime dt, string deviceId);

        ICollection<Box> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId);

        ICollection<Box> FindBy(DateTime startTime, DateTime endTime, string deviceId, int? boxId);

        Task<ICollection<Box>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? boxId);

        Task<ICollection<Box>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId, int? boxId);

        int Delete(string ymd, string hms, string deviceId);
    }
}
