using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.LoggerService;
using FluentEmail.Core;
using FluentEmail.Core.Models;

namespace CSG.MI.TrMontrgSrv.Web.Montrg
{
    // https://blog.zhaytam.com/2019/06/08/emailsender-service-fluent-email-razor-templates/
    public class EmailSender : IEmailSender
    {
        #region Fields

        private const string TEMPLATE_PATH = "CSG.MI.TrMontrgSrv.Web.Montrg.Templates.{0}.cshtml";
        private readonly IFluentEmail _email;
        private readonly ILoggerManager _logger;

        #endregion

        #region Constructors

        public EmailSender(IFluentEmail email,
                           ILoggerManager logger)
        {
            _email = email;
            _logger = logger;
        }

        #endregion

        #region Public Methods

        public async Task<bool> SendTemplate(IEnumerable<Address> to, string subject, string template, object model)
        {
            var result = await _email.To(to)
                                     .Subject(subject)
                                     .UsingTemplate(template, model)
                                     .SendAsync();

            if (result.Successful)
            {
                _logger.LogInfo($"Sent an {template} email to {String.Join(",", to.Select(x => x.EmailAddress))} successfully");
            }
            else
            {
                var msg = String.Join(Environment.NewLine, result.ErrorMessages);
                _logger.LogError($"Failed to send an email.\n\r{msg}");
            }

            return result.Successful;
        }

        public async Task<bool> SendUsingTemplateFromEmbedded(string to, string subject, EmailTemplate template, object model)
        {
            var result = await _email.To(to)
                                     .Subject(subject)
                                     .UsingTemplateFromEmbedded(String.Format(TEMPLATE_PATH, template),
                                                                ToExpando(model),
                                                                this.GetType().GetTypeInfo().Assembly)
                                     .SendAsync();
            if (result.Successful)
            {
                _logger.LogInfo($"Sent an {template.ToString()} email to {to} successfully");
            }
            else
            {
                var msg = String.Join(Environment.NewLine, result.ErrorMessages);
                _logger.LogError($"Failed to send an email.\n\r{msg}");
            }

            return result.Successful;
        }

        public async Task<bool> SendUsingTemplateFromEmbedded(IEnumerable<Address> to, string subject, EmailTemplate template, object model)
        {
            var path = String.Format(TEMPLATE_PATH, template);
            var assembly = this.GetType().GetTypeInfo().Assembly;

            // test
            //using (var stream = assembly.GetManifestResourceStream(path))
            //using (var reader = new StreamReader(stream))
            //{
            //    string str = reader.ReadToEnd();
            //}

            var result = await _email.To(to)
                                     .Subject(subject)
                                     .UsingTemplateFromEmbedded(path,
                                                                ToExpando(model),
                                                                assembly,
                                                                true)
                                     .SendAsync();
            if (result.Successful)
            {
                _logger.LogInfo($"Sent an {template} email to {String.Join(",", to.Select(x => x.EmailAddress))} successfully");
            }
            else
            {
                var msg = String.Join(Environment.NewLine, result.ErrorMessages);
                _logger.LogError($"Failed to send an email.\n\r{msg}");
            }

            return result.Successful;
        }

        public async Task<bool> SendUsingTemplateFromEmbedded(IEnumerable<Address> to,
                                                              IEnumerable<Address> bcc,
                                                              string subject,
                                                              EmailTemplate template,
                                                              object model)
        {
            var path = String.Format(TEMPLATE_PATH, template);
            var assembly = this.GetType().GetTypeInfo().Assembly;

            var result = await _email.To(to)
                                     .BCC(bcc)
                                     .Subject(subject)
                                     .UsingTemplateFromEmbedded(path,
                                                                ToExpando(model),
                                                                assembly,
                                                                true)
                                     .SendAsync();
            if (result.Successful)
            {
                _logger.LogInfo($"Sent an {template} email to {String.Join(",", to.Select(x => x.EmailAddress))} " +
                                Environment.NewLine +
                                $"with bcc {String.Join(",", bcc.Select(x => x.EmailAddress))} successfully");
            }
            else
            {
                var msg = String.Join(Environment.NewLine, result.ErrorMessages);
                _logger.LogError($"Failed to send an email.\n\r{msg}");
            }

            return result.Successful;
        }

        public async Task<bool> SendUsingTemplateFromEmbedded(IEnumerable<Address> to,
                                                              IEnumerable<Address> cc,
                                                              IEnumerable<Address> bcc,
                                                              string subject,
                                                              EmailTemplate template,
                                                              object model)
        {
            var result = await _email.To(to)
                                     .CC(cc)
                                     .BCC(bcc)
                                     .Subject(subject)
                                     .UsingTemplateFromEmbedded(String.Format(TEMPLATE_PATH, template),
                                                                ToExpando(model),
                                                                this.GetType().GetTypeInfo().Assembly)
                                     .SendAsync();
            if (result.Successful)
            {
                _logger.LogInfo($"Sent an {template} email to {String.Join(",", to.Select(x => x.EmailAddress))} " +
                                Environment.NewLine +
                                $"with cc {String.Join(",", cc.Select(x => x.EmailAddress))} " +
                                Environment.NewLine +
                                $"with bcc {String.Join(",", bcc.Select(x => x.EmailAddress))} " +
                                Environment.NewLine +
                                "successfully");
            }
            else
            {
                var msg = String.Join(Environment.NewLine, result.ErrorMessages);
                _logger.LogError($"Failed to send an email.\n\r{msg}");
            }

            return result.Successful;
        }

        #endregion

        #region Private Methods

        private static ExpandoObject ToExpando(object model)
        {
            if (model is ExpandoObject exp)
            {
                return exp;
            }

            IDictionary<string, object> expando = new ExpandoObject();
            var properties = model.GetType()
                                  .GetTypeInfo()
                                  .GetProperties();
                                  //.Where(p => p.GetIndexParameters().Any() == false);

            foreach (var propertyDescriptor in properties)
            {
                var obj = propertyDescriptor.GetValue(model);

                if (obj != null && IsAnonymousType(obj.GetType()))
                {
                    obj = ToExpando(obj);
                }

                expando.Add(propertyDescriptor.Name, obj);
            }

            return (ExpandoObject)expando;
        }

        private static bool IsAnonymousType(Type type)
        {
            bool hasCompilerGeneratedAttribute = type.GetTypeInfo()
                .GetCustomAttributes(typeof(CompilerGeneratedAttribute), false)
                .Any();

            bool nameContainsAnonymousType = type.FullName.Contains("AnonymousType");
            bool isAnonymousType = hasCompilerGeneratedAttribute && nameContainsAnonymousType;

            return isAnonymousType;
        }

        #endregion
    }
}
