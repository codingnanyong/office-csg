using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model
{
    public class MailingAddr : BaseModel
    {
        public string PlantId { get; set; }

        public string Email { get; set; }

        public string Name { get; set; }

        public string Telephone { get; set; }

        public string Team { get; set; }

        public bool Inactive { get; set; }
    }
}
