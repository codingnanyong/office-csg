using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;

namespace CSG.MI.FDW.EF.Entities.Feedback
{
    [Table("system", Schema = "feedback")]
    public class SystemEntity : BaseEntity
    {
        [Key, Column("id", TypeName = "integer")]
        public int Id { get; set; }

        [Column("name", TypeName = "varchar(20)")]
        public string Name { get; set; } = String.Empty;
    }
}
