using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.Interface;

namespace CSG.MI.TrMontrgSrv.BLL.Interface
{
    public interface IImrCtrlRepo
    {
        IImrCtrl Get(string deviceId, int? id);
    }
}
