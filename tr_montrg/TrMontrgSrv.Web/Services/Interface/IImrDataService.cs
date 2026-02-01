using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.ApiData;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IImrDataService : IDataService
    {
        Task<List<ImrData>> GetFrameImrData(string deviceId, string start, string end);

        Task<List<ImrData>> GetRoiImrData(int roiId, string deviceId, string start, string end);
    }
}
