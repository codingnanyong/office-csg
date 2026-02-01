using CSG.MI.DAO.Production.PCC;

namespace CSG.MI.FDW.BLL.Production.PCC.Interface
{
    public interface IOpMstRepo : IDisposable
    {
        ICollection<OpMst> GetAll();

        Task<ICollection<OpMst>> GetAllAsync();

        OpMst Get(string code, string factory = "DS");

        Task<OpMst> GetAsync(string code, string factory = "DS");
    }
}
