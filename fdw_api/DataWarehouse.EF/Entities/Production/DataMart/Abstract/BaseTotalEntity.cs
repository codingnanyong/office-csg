using System.ComponentModel.DataAnnotations.Schema;

namespace Entities.Production.DataMart.Abstract
{
    public abstract class BaseTotalEntity
    {
        [Column("year", TypeName = "text")]
        public string Year { get; set; } = string.Empty;

        [Column("factory", TypeName = "varchar(5)")]
        public string Factory { get; set; } = string.Empty;

        [Column("cnt", TypeName = "bigint")]
        public long TotalCnt { get; set; } = long.MinValue;

        [Column("qty", TypeName = "numeric")]
        public decimal TotalQty { get; set; } = decimal.Zero;
    }
}
