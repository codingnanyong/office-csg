using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Feedback
{
    [Table("user_feedback", Schema = "feedback")]
	public class FeedbackEntity : BaseEntity
	{
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        [Column("seq", TypeName = "BigInt")]
        public long? Seq { get; set; }

        [Key, Column("system", TypeName = "integer")]
		public int System { get; set; }

        [Column("category", TypeName = "varchar(30)")]
		public string Category { get; set; } = String.Empty;

        [Column("comment", TypeName = "text")]
		public string? Comment { get; set; } = String.Empty;

        [Key,Column("edit_dt", TypeName = "timestamp")]
		public DateTime EditDate { get; set; } = DateTime.Now;

		/// <summary>
		/// Foreign Keys
		/// </summary>

        [ForeignKey("Category")]
		public CategoryEntity? CategoryNavigation { get; set; }

		[ForeignKey("System")]
		public SystemEntity? SystemNavigation { get; set; }
	}
}
