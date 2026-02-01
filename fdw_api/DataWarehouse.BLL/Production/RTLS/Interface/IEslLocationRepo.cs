using CSG.MI.DAO.Production.RTLS;

namespace CSG.MI.FDW.BLL.Production.RTLS.Interface
{
    public interface IEslLocationRepo : IDisposable
    {
        ICollection<EslLocation> GetCurrentAll();

        Task<ICollection<EslLocation>> GetCurrentAllAsync();

        EslLocation GetCurrent(string tagid);

        Task<EslLocation> GetCurrentAsync(string tagid);
    }
}
