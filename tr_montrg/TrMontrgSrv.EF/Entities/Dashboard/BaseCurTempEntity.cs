using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;

namespace CSG.MI.TrMontrgSrv.EF.Entities.Dashboard
{
    public abstract class BaseCurTempEntity : BaseEntity
    {
        [Column("device_id", TypeName = "varchar(20)"), Comment("Device ID")]
        public string DeviceId { get; set; }

        [Column("id", TypeName = "integer")]
        public int? Id { get; set; } = 0;

        [Column("avg", TypeName = "real")]
        public float? Avg { get; set; } = 0;

        [Column("max", TypeName = "real")]
        public float? Max { get; set; } = 0;

        [Column("min", TypeName = "real")]
        public float? Min { get; set; } = 0;

        [Column("dif", TypeName = "real")]
        public float? Dif { get; set; } = 0;

        /* 
         * TODO : 전 시간과 비교 Data 추가시 주석 해제
        [Column("dif_avg", TypeName = "real")]
        public float? d_Avg { get; set; } = 0;

        [Column("dif_max", TypeName = "real")]
        public float? d_Max { get; set; } = 0;

        [Column("dif_min", TypeName = "real")]
        public float? d_Min { get; set; } = 0;

        [Column("dif_dif", TypeName = "real")]
        public float? d_Diff { get; set; } = 0;
        */

        // Table일 경우에 Relationship
        //public virtual CurDeviceEntity CurDevice { get; set; }
    }
}
