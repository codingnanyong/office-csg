using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.FDW.EF.Entities.Production.DataMart
{
    [Table("worksheets_coordinate", Schema = "services")]
    public class WsCoodinateEntity : BaseEntity
    {
        [Column("wsno", TypeName = "text")]
        public string WsNo { get; set; } = String.Empty;

        [Column("tagid", TypeName = "text")]
        public string tagId { get; set; } = String.Empty;

        [Column("opcd", TypeName = "text")]
        public string? opcd { get; set; } = String.Empty;

        [Column("status", TypeName = "text")]
        public string? Status { get; set; } = String.Empty;

        [Column("passtype", TypeName = "text")]
        public string PassType { get; set; } = String.Empty;

        [Column("event_id", TypeName = "text")]
        public string EventId { get; set; } = String.Empty;

        [Column("position_x", TypeName = "text")]
        public string? x { get; set; } = String.Empty;

        [Column("position_y", TypeName = "text")]
        public string? y { get; set; } = String.Empty;

        [Column("coordi_sys", TypeName = "text")]
        public string Floor { get; set; } = String.Empty;
    }
}
