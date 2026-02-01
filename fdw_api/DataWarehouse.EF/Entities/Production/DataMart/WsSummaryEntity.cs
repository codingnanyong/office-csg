using CSG.MI.FDW.EF.Entities.Production.DataMart.Abstract;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("worksheets_summary", Schema = "services")]
    public class WsSummaryEntity : BaseWSListEntity
    {
    }
}
