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
    [Table("mailing_addr", Schema = "public")]
    public class MailingAddrEntity : BaseEntity
    {
        [Key, Column("plant_id", TypeName = "varchar(10)"), Comment("Plant ID")]
        public string PlantId { get; set; }

        [Key, Column("email", TypeName = "varchar(100)"), Comment("Email")]
        public string Email { get; set; }

        [Column("name", TypeName = "varchar(30)"), Required, Comment("Full name")]
        public string Name { get; set; }

        [Column("tel", TypeName = "varchar(30)"), Comment("Telephone number")]
        public string Telephone { get; set; }

        [Column("team", TypeName = "varchar(30)"), Comment("Team name")]
        public string Team { get; set; }

        [Column("inactive", TypeName = "boolean"), Required, Comment("Inactive status")]
        public bool Inactive { get; set; }

    }
}
