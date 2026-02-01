using CSG.MI.DAO.Production.PCC;

namespace CSG.MI.FDW.BLL.Production.PCC.Interface
{
    public interface IMachineMstRepo : IDisposable
    {
        ICollection<MachineMst> GetAll();

        Task<ICollection<MachineMst>> GetAllAsync();

        MachineMst Get(decimal seq);

        Task<MachineMst> GetAsync(decimal seq);

    }
}
