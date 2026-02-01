using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("worksheets", Schema = "services")]
    public class WsEntity : BaseEntity
    {
        [Column("factory", TypeName = "text")]
        public string Factory { get; set; } = String.Empty;

        [Column("wsno", TypeName = "text")]
        public string WsNo { get; set; } = String.Empty;
    }
}
