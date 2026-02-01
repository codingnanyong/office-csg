using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.BLL.AutoBatch.Interface
{
    public interface IInspectionRepo : IDisposable
    {
        ICollection<InspecDevice> GetCheck();

        Task<ICollection<InspecDevice>> GetCheckAsync();

        ICollection<InspecDevice> GetTest();
    }
}
