using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities
{
	public abstract class BaseMstEntity
	{
		[Key, Column("key", TypeName = "varchar(5)")]
		public string Key { get; set; } = String.Empty;

		[Column("value_kor", TypeName = "varchar(30)")]
		public string Value_Kor { get; set; } = String.Empty;

        [Column("value_eng", TypeName = "varchar(30)")]
		public string Value_Eng { get; set; } = String.Empty;

        [Column("edit_dt", TypeName = "timestamp")]
		public DateTime EditDate { get; set; } = DateTime.Now;
	}
}