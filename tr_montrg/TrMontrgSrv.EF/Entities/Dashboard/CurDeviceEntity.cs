using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.TrMontrgSrv.EF.Entities.Dashboard
{
    [Table("fact_device", Schema = "public")]
    public class CurDeviceEntity : BaseEntity
    {
        [Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("loc_id", TypeName = "varchar(10)"), Comment("Location ID")]
        public string LocationId { get; set; }

        [Column("plant_id", TypeName = "varchar(10)"), Comment("Plant ID")]
        public string PlantId { get; set; }

        [Column("desn", TypeName = "varchar(200)"), Comment("Description")]
        public string Description { get; set; }

        /* Table일 경우 Relationship
        
        public virtual CurFrameEntity Frame { get; set; }

        public virtual ICollection<CurBoxEntity> Boxes { get; set; }

        public virtual ICollection<CurRoiEntity> Roies { get; set; }
        */
    }
}
