using CSG.MI.DAO.Production.PCC;

namespace CSG.MI.FDW.BLL.Production.PCC.Interface
{
    public interface IIssueMstRepo : IDisposable
    {
        ICollection<IssueMst> GetAll();

        Task<ICollection<IssueMst>> GetAllAsync();

        IssueMst Get(string code, string factory = "DS");

        Task<IssueMst> GetAsync(string code, string factory = "DS");
    }
}
