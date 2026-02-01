using System.ComponentModel.DataAnnotations.Schema;
using Entities.Production.DataMart.Abstract;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("pln_tot", Schema = "services")]
    public class FactoryPlnTotalEntity : BaseTotalEntity
    {
    }
}
