using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model.Interface
{
    public interface IImrCtrl
    {
        float? UclIMax { get; set; }

        float? LclIMax { get; set; }

        float? UclMrMax { get; set; }

        float? UclIDiff { get; set; }

        float? LclIDiff { get; set; }

        float? UclMrDiff { get; set; }

        float? TWarning { get; set; }
    }
}
