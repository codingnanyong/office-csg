using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IEvntLogDataService
    {
        Task<bool> Exists(string deviceId, DateTime dt);

        Task<EvntLog> Get(int id);

        Task<List<EvntLog>> GetList(string deviceId, string start = null, string end = null);

        Task<List<EvntLog>> GetList(string deviceId, string evntLevel, string start, string end);

        Task<List<EvntLog>> GetListByPlant(string plantId, string start = null, string end = null);

        Task<List<EvntLog>> GetLatest(string plantId, string deviceId = null, bool excludingInfoLevel = false);

        Task<EvntLog> Create(EvntLog evntLog);

        Task<bool> Update(EvntLog evntLog);

        Task<bool> Delete(int id);
    }
}
