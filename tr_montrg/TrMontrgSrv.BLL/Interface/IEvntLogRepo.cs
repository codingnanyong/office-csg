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
    public interface IEvntLogRepo : IDisposable
    {
        bool Exists(int id);

        bool Exists(string deviceId, string ymd, string hms);

        EvntLog Get(int id);

        Task<EvntLog> GetAsync(int id);

        List<EvntLog> GetLastest(string plantId, string deviceId = null, bool excludingInfoLevel = false);

        List<EvntLog> FindBy(string deviceId);

        List<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        List<EvntLog> FindBy(string startYmd, string startHms, string endYmd, string endHms, string deviceId, string evntLvl);

        List<EvntLog> FindBy(DateTime startTime, DateTime endTime, string deviceId);

        Task<ICollection<EvntLog>> FindByAsync(string startYmd, string startHms, string endYmd, string endHms, string deviceId);

        Task<ICollection<EvntLog>> FindByAsync(DateTime startTime, DateTime endTime, string deviceId);

        List<EvntLog> FindByPlant(string plantId);

        List<EvntLog> FindByPlant(string startYmd, string startHms, string endYmd, string endHms, string plantId);

        EvntLog Create(EvntLog evntLog);

        Task<EvntLog> CreateAsync(EvntLog evntLog);

        EvntLog Update(EvntLog evntLog);

        Task<EvntLog> UpdateAsync(EvntLog evntLog);

        int Delete(int id);

        Task<int> DeleteAsync(int id);
    }
}
