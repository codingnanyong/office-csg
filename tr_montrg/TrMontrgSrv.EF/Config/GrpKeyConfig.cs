using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.EF.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CSG.MI.TrMontrgSrv.EF.Config
{
    public class GrpKeyConfig : IEntityTypeConfiguration<GrpKeyEntity>
    {
        public void Configure(EntityTypeBuilder<GrpKeyEntity> builder)
        {
            // GrpKey Entity
            builder.HasKey(gk => new { gk.Group, gk.Key })
                   .HasName("grp_key_pkey");

            builder.HasData(
                    new GrpKeyEntity { Group = "plant_id", Key = "HQ" },
                    new GrpKeyEntity { Group = "plant_id", Key = "VJ" },
                    new GrpKeyEntity { Group = "plant_id", Key = "JJ" },
                    new GrpKeyEntity { Group = "plant_id", Key = "CKP" },
                    new GrpKeyEntity { Group = "plant_id", Key = "JJS" },
                    new GrpKeyEntity { Group = "plant_id", Key = "RJ" },
                    new GrpKeyEntity { Group = "plant_id", Key = "QD" },
                    new GrpKeyEntity { Group = "medium_type", Key = "ir", Description = "IR camera image" },
                    new GrpKeyEntity { Group = "medium_type", Key = "rgb", Description = "RGB camera image" },
                    new GrpKeyEntity { Group = "medium_type", Key = "cfg", Description = "Confiruation file" },
                    new GrpKeyEntity { Group = "medium_type", Key = "temp", Description = "Temperature summary json" },
                    new GrpKeyEntity { Group = "medium_type", Key = "raw", Description = "Temperature raw csv" },
                    new GrpKeyEntity { Group = "file_type", Key = "json" },
                    new GrpKeyEntity { Group = "file_type", Key = "csv" },
                    new GrpKeyEntity { Group = "file_type", Key = "jpg" },
                    new GrpKeyEntity { Group = "event_type", Key = "MaxTemp", Description = "Max. temperature" },
                    new GrpKeyEntity { Group = "event_type", Key = "DiffLevel", Description = "Diff. level" },
                    new GrpKeyEntity { Group = "event_type", Key = "UCL", Description = "Upper control limit" },
                    new GrpKeyEntity { Group = "event_type", Key = "LCL", Description = "Lower control limit" },
                    new GrpKeyEntity { Group = "event_type", Key = "NelsonRule", Description = "Nelson rule" },
                    new GrpKeyEntity { Group = "event_lvl", Key = "Urgent" },
                    new GrpKeyEntity { Group = "event_lvl", Key = "Warning" },
                    new GrpKeyEntity { Group = "event_lvl", Key = "Info" },
                    new GrpKeyEntity { Group = "event_lvl", Key = "Error" }
                );
        }
    }
}
