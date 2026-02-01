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
    public interface IEvntLogRepository : IGenericMapRepository<EvntLogEntity, EvntLog>
    {
        bool Exists(int id);

        bool Exists(string deviceId, string ymd, string hms);

        EvntLog Get(int id);

        Task<EvntLog> GetAsync(int id);

        List<EvntLog> GetLastest(string plantId, string deviceId = null, bool excludingInfoLevel = false);

        ICollection<EvntLog> FindBy(string deviceId);

        ICollection<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        ICollection<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string evntLvl);

        ICollection<EvntLog> FindBy(DateTime startTime, DateTime endTime, string deviceId);

        Task<ICollection<EvntLog>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        Task<ICollection<EvntLog>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId);

        ICollection<EvntLog> FindByPlant(string plantId);

        ICollection<EvntLog> FindByPlant(string startYmd, string startHms, string endYmd, string endHms, string plantId);

        EvntLog Create(EvntLog evntLog);

        Task<EvntLog> CreateAsync(EvntLog evntLog);

        EvntLog Update(EvntLog evntLog);

        Task<EvntLog> UpdateAsync(EvntLog evntLog);

        int Delete(int id);

        Task<int> DeleteAsync(int id);
    }
}
