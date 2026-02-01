using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Department
{
    [Table("dept_rnr", Schema = "dept")]
    public class DeptRnREntity : BaseEntity
    {
        [Key, Column("dept_id", TypeName = "varchar(5)")]
        public string DeptId { get; set; } = String.Empty;

        [Column("ability", TypeName = "varchar(50)")]
        public string? Ability { get; set; } = String.Empty;

        [Column("job", TypeName = "text")]
        public string? Job { get; set; } = String.Empty;

        [Column("description", TypeName = "text")]
        public string? Description { get; set; } = String.Empty;

        [Column("edit_dt", TypeName = "timestamp")]
        public DateTime EditDate { get; set; } = DateTime.Now;

        [ForeignKey("DeptId")]
        public DeptEntity? DeptNavigation { get; set; }

    }
}
