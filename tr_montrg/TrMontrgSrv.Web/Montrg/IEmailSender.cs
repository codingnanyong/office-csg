using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentEmail.Core.Models;

namespace CSG.MI.TrMontrgSrv.Web.Montrg
{
    public interface IEmailSender
    {
        Task<bool> SendTemplate(IEnumerable<Address> to, string subject, string template, object model);

        Task<bool> SendUsingTemplateFromEmbedded(string to, string subject, EmailTemplate template, object model);

        Task<bool> SendUsingTemplateFromEmbedded(IEnumerable<Address> to, string subject, EmailTemplate template, object model);

        Task<bool> SendUsingTemplateFromEmbedded(IEnumerable<Address> to,
                                                 IEnumerable<Address> cc,
                                                 IEnumerable<Address> bcc,
                                                 string subject,
                                                 EmailTemplate template,
                                                 object model);
    }
}
