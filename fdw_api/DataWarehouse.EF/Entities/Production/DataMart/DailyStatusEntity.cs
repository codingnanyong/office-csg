using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("dailystatus", Schema = "services")]
    public class DailyStatusEntity : BaseEntity
    {
        [Column("factory", TypeName = "text")]
        public string Factory { get; set; } = String.Empty;

        [Column("opcd", TypeName = "text")]
        public string OpCd { get; set; } = String.Empty;

        [Column("opname", TypeName = "text")]
        public string OpName { get; set; } = String.Empty;

        [Column("oplocalname", TypeName = "text")]
        public string OpLocalName { get; set; } = String.Empty;

        [Column("plancnt", TypeName = "numeric")]
        public decimal? PlanCnt { get; set; } = Decimal.Zero;

        [Column("planqty", TypeName = "numeric")]
        public decimal? PlanQty { get; set; } = Decimal.Zero;

        [Column("prodcnt", TypeName = "numeric")]
        public decimal? ProdCnt { get; set; } = Decimal.Zero;

        [Column("prodqty", TypeName = "numeric")]
        public decimal? ProdQty { get; set; } = Decimal.Zero;

        [Column("rate", TypeName = "numeric")]
        public decimal? Rate { get; set; } = Decimal.Zero;
    }
}
