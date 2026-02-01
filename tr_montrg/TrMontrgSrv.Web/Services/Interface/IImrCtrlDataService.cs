using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.ApiData;

namespace CSG.MI.TrMontrgSrv.Web.Services.Interface
{
    public interface IImrCtrlDataService
    {
        Task<ImrCtrl> GetImrCtrl(string deviceId, int? roiId);
    }
}
