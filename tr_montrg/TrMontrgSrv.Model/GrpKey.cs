using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class GrpKey : BaseModel
    {
        [Required(ErrorMessage = "Group field is required.")]
        [StringLength(30)]
        public string Group { get; set; }

        [Required(ErrorMessage = "Key field is required.")]
        [StringLength(50)]
        public string Key { get; set; }

        [StringLength(100)]
        public string Description { get; set; }

        public int? Order { get; set; }

        [Required(ErrorMessage = "Inactive field is required.")]
        public bool Inactive { get; set; }
    }
}
