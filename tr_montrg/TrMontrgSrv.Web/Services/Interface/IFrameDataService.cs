using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IFrameDataService : IDataService
    {
        Task<List<Frame>> GetList(string deviceId, string start, string end);
    }
}
