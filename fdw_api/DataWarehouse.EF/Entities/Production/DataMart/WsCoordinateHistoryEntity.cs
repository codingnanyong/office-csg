using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("worksheets_coordinate_history",Schema ="services")]
    public class WsCoordinateHistoryEntity : BaseEntity
    {
        [Column("wsno",TypeName ="varchar(20)")]
        public string WsNo { get; set; } = String.Empty;

        [Column("tagid", TypeName = "varchar(12)")]
        public string? TagId { get; set; } = String.Empty;

        [Column("tag_type", TypeName = "varchar(1)")]
        public string? Type { get; set; } = String.Empty;

        [Column("opcd",TypeName ="varchar(10)")]
        public string? Opcd {  get; set; } = String.Empty;

        [Column("status", TypeName = "varchar(1)")]
        public string? Status { get; set; } = String.Empty;

        [Column("zone", TypeName = "varchar(12)")]
        public string? Zone { get; set; } = String.Empty;

        [Column("floor",TypeName= "varchar(30)")]
        public string? Floor { get; set; } = String.Empty;

        [Column("now", TypeName = "timestamp")]
        public DateTime Now { get; set; } = DateTime.Now;

        [Column("next", TypeName = "timestamp")]
        public DateTime Next { get; set; } = DateTime.Now;

        [Column("diff", TypeName = "interval")]
        public TimeSpan? LeadTime { get; set; } = TimeSpan.MinValue;

    }
}
