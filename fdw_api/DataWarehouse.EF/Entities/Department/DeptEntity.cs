using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Department
{
    [Table("dept_mst", Schema = "dept")]
    public class DeptEntity : BaseEntity
    {
        [Key, Column("dept_id", TypeName = "varchar(5)")]
        public string DeptId { get; set; } = String.Empty;

        [Column("dept_name", TypeName = "varchar(20)")]
        public string DeptName { get; set; } = String.Empty;

        [Column("dept_cmp", TypeName = "varchar(5)")]
        public string? Dept_Company { get; set; } = String.Empty;

        [Column("dept_org", TypeName = "varchar(5)")]
        public string? Dept_Organization { get; set; } = String.Empty;

        [Column("dept_cntr", TypeName = "varchar(5)")]
        public string? Dept_Central { get; set; } = String.Empty;

        [Column("dept_grp", TypeName = "varchar(5)")]
        public string? Dept_Group { get; set; } = String.Empty;

        [Column("dept_team", TypeName = "varchar(5)")]
        public string? Dept_Team { get; set; } = String.Empty;

        [Column("edit_dt", TypeName = "timestamp")]
        public DateTime EditDate { get; set; } = DateTime.Now;
    }
}
