using CSG.MI.DAO.Production.RTLS;

namespace CSG.MI.FDW.BLL.Production.RTLS.Interface
{
    public interface IProcessMstRepo : IDisposable
    {
        ICollection<ProcessMst> GetAll();

        Task<ICollection<ProcessMst>> GetAllAsync();

        ProcessMst Get(string processloc);

        Task<ProcessMst> GetAsync(string processloc);
    }
}