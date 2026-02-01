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
    public class MailingAddrConfig : IEntityTypeConfiguration<MailingAddrEntity>
    {
        public void Configure(EntityTypeBuilder<MailingAddrEntity> builder)
        {
            // MailingAddr Entity
            builder.HasKey(ma => new { ma.PlantId, ma.Email })
                   .HasName("mailing_addr_pkey");
        }
    }
}
