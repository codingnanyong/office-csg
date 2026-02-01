using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.EF.Entities
{
    // v1.1
    [Table("grp_key", Schema = "public")]
    public class GrpKeyEntity : BaseEntity
    {
        [Key, Column("grp", TypeName = "varchar(30)"), Comment("Group name")]
        public string Group { get; set; }

        [Key, Column("key", TypeName = "varchar(50)"), Comment("Key value")]
        public string Key { get; set; }

        [Column("desn", TypeName = "varchar(100)"), Comment("Description")]
        public string Description { get; set; }

        [Column("ord", TypeName = "integer"), Comment("Order by")]
        public int? Order { get; set; }

        [Column("inactive", TypeName = "boolean"), Required, Comment("Inactive status")]
        public bool Inactive { get; set; }
    }
}
