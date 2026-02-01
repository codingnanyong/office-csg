using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model.Interface;

namespace CSG.MI.TrMontrgSrv.Model.ApiData
{
    public class ImrCtrl : IImrCtrl
    {
        public float? UclIMax { get; set; }

        public float? LclIMax { get; set; }

        public float? UclMrMax { get; set; }

        public float? UclIDiff { get; set; }

        public float? LclIDiff { get; set; }

        public float? UclMrDiff { get; set; }

        public float? TWarning { get; set; }
    }
}
