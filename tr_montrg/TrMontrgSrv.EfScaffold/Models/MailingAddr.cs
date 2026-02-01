using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("mailing_addr")]
    public partial class MailingAddr
    {
        [Key]
        [Column("plant_id")]
        [StringLength(10)]
        public string PlantId { get; set; }
        [Key]
        [Column("email")]
        [StringLength(100)]
        public string Email { get; set; }
        [Required]
        [Column("name")]
        [StringLength(30)]
        public string Name { get; set; }
        [Column("tel")]
        [StringLength(30)]
        public string Tel { get; set; }
        [Column("team")]
        [StringLength(30)]
        public string Team { get; set; }
        [Column("inactive")]
        public bool Inactive { get; set; }
    }
}
