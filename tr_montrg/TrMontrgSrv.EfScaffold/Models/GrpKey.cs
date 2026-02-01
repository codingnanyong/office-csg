using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

#nullable disable

namespace CSG.MI.TrMontrgSrv.EfScaffold.Models
{
    [Table("grp_key")]
    public partial class GrpKey
    {
        [Key]
        [Column("grp")]
        [StringLength(30)]
        public string Grp { get; set; }
        [Key]
        [Column("key")]
        [StringLength(50)]
        public string Key { get; set; }
        [Column("desn")]
        [StringLength(100)]
        public string Desn { get; set; }
        [Column("ord")]
        public int? Ord { get; set; }
        [Column("inactive")]
        public bool Inactive { get; set; }
    }
}
