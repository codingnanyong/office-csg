using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities
{
	public class BaseTotalEntity
	{
		[Column("year", TypeName = "text")]
		public string Year { get; set; } = String.Empty;

        [Column("factory", TypeName = "varchar(5)")]
		public string Factory { get; set; } = String.Empty;

        [Column("cnt", TypeName = "bigint")]
		public long TotalCnt { get; set; } = long.MinValue;

		[Column("qty", TypeName = "numeric")]
		public decimal TotalQty { get; set; } = Decimal.Zero;
	}
}
