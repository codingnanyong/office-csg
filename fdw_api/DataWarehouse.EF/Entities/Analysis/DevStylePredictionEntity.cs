using System.ComponentModel.DataAnnotations.Schema;
using CSG.MI.FDW.EF.Entities;

namespace Entities.Analysis
{
    [Table("prediction_devstyle", Schema = "services")]
    public class DevStylePredictionEntity : BaseEntity
    {
        [Column("dev_style_number", TypeName = "varchar(30)")]
        public string DevStyleNumber { get; set; } = string.Empty;

        [Column("tag_type", TypeName = "varchar(1)")]
        public string TagType { get; set; } = string.Empty;

        [Column("opcd", TypeName = "varchar(10)")]
        public string OpCode { get; set; } = string.Empty;

        [Column("time", TypeName = "timestamp")]
        public DateTime Time { get; set; } = DateTime.Now;
    }
}
