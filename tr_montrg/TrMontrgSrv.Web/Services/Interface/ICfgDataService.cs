using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface ICfgDataService : IDataService
    {
        Task<Cfg> Get(string deviceId);

        Task<Dictionary<int, int[]>> GetCfgRois(string deviceId);
    }
}
