using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Core
{
    public enum DbHandlerResult
    {
        Unknown = -1,
        Success,
        UnzipFailure,
        DataLoadFailure,
        DataInsertFailure,
        DirDeleteFailure,
        FileMoveFailure
    }
}
