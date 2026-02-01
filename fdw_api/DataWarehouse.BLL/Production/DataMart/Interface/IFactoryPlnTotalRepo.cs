using CSG.MI.DTO.Production;

namespace CSG.MI.FDW.BLL.Production.DataMart.Interface
{
    public interface IFactoryPlnTotalRepo : IDisposable
    {
        ICollection<FactoryTotal> GetPlans();

        Task<ICollection<FactoryTotal>> GetPlansAsync();

        ICollection<FactoryTotal> GetByFactory(string factory = "DS");

        Task<ICollection<FactoryTotal>> GetByFactoryAsync(string factory = "DS");

        FactoryTotal GetByYearFactory(string year, string factory = "DS");

        Task<FactoryTotal> GetByYearFactoryAsync(string year, string factory = "DS");
    }
}
