using CSG.MI.TrMontrgSrv.EF.Core.Repo;
using CSG.MI.TrMontrgSrv.EF.Entities;
using CSG.MI.TrMontrgSrv.Model.AutoBatch;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.EF.Repositories.AutoBatch.Interface
{
    public interface IInspectionRepository : IGenericMapRepository<DeviceEntity, InspecDevice>
    {
        ICollection<InspecDevice> GetCheck();

        Task<ICollection<InspecDevice>> GetCheckAsync();

        ICollection<InspecDevice> GetTest();
    }
}
