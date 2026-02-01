using System;
using System.Collections.Generic;
using CSG.MI.TrMontrgSrv.EF;
using CSG.MI.TrMontrgSrv.EF.Repositories;
using CSG.MI.TrMontrgSrv.Helpers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace CSG.MI.TrMontrgSrv.SL.RepoServices
{
    public static class RepoFactory
    {
        private static readonly Dictionary<Type, Type> _registeredTypes = new();
        private static readonly IConfigurationRoot _configuration;
        private static readonly DbContextOptionsBuilder<AppDbContext> _optionsBuilder;

        static RepoFactory()
        {
            _configuration = ConfigurationBuilderSingleton.ConfigurationRoot;
            _optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
            _optionsBuilder.UseNpgsql(_configuration["Data:TrMontrgSrv:ConnectionString"]);

            _registeredTypes.Add(typeof(DeviceRepository), typeof(Repo<DeviceRepository>));
            _registeredTypes.Add(typeof(FrameRepository), typeof(Repo<FrameRepository>));
            _registeredTypes.Add(typeof(RoiRepository), typeof(Repo<RoiRepository>));
            _registeredTypes.Add(typeof(BoxRepository), typeof(Repo<BoxRepository>));
            _registeredTypes.Add(typeof(CfgRepository), typeof(Repo<CfgRepository>));
            _registeredTypes.Add(typeof(MediumRepository), typeof(Repo<MediumRepository>));

            _registeredTypes.Add(typeof(DeviceCtrlRepository), typeof(Repo<DeviceCtrlRepository>));
            _registeredTypes.Add(typeof(FrameCtrlRepository), typeof(Repo<FrameCtrlRepository>));
            _registeredTypes.Add(typeof(RoiCtrlRepository), typeof(Repo<RoiCtrlRepository>));
            _registeredTypes.Add(typeof(BoxCtrlRepository), typeof(Repo<BoxCtrlRepository>));
            _registeredTypes.Add(typeof(EvntLogRepository), typeof(Repo<EvntLogRepository>));
            _registeredTypes.Add(typeof(MailingAddrRepository), typeof(Repo<MailingAddrRepository>));
            _registeredTypes.Add(typeof(GrpKeyRepository), typeof(Repo<GrpKeyRepository>));
        }

        public static Repo<T> Get<T>() where T : class, IDisposable
        {
            var t = typeof(T);

            if (_registeredTypes.ContainsKey(t) == false)
                throw new NotSupportedException();

            var typeToCreate = _registeredTypes[t];

            return Activator.CreateInstance(typeToCreate, _optionsBuilder.Options) as Repo<T>;
        }

    }
}
