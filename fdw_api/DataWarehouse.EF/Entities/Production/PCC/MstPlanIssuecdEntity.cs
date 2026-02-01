using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.PCC
{
    [Table("pcc_mst_plan_issue_cd", Schema = "pcc")]
    public class MstPlanIssuecdEntity : BaseEntity
    {
        [Key, Column("factory", TypeName = "varchar(5)")]
        public string Factory { get; set; } = String.Empty;

        [Key, Column("issue_cd", TypeName = "varchar(10)")]
        public string IssueCd { get; set; } = String.Empty;

        [Column("issue_level", TypeName = "numeric")]
        public decimal IssueLevel { get; set; } = Decimal.Zero;

        [Column("issue_name", TypeName = "varchar(50)")]
        public string IssueName { get; set; } = String.Empty;

        [Column("parent_issue_cd", TypeName = "varchar(10)")]
        public string? ParentIssueCd { get; set; } = String.Empty;

        [Column("use_yn", TypeName = "varchar(1)")]
        public string UseYN { get; set; } = String.Empty;
    }
}
