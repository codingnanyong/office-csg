using AutoMapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace CSG.MI.FDW.EF.Data
{
    public class BaseDbContext : DbContext
    {
        #region Fields
        
        public static readonly ILoggerFactory AppLoggerFactory = LoggerFactory.Create(builder => { builder.AddConsole(); });
        public IMapper Mapper { get; private set; }

        #endregion

        #region Constructors

        public BaseDbContext(DbContextOptions options) : base(options)
        {
            Mapper = ConfigureMapper();
        }

        private IMapper ConfigureMapper()
        {
            var config = new MapperConfiguration(cfg =>
            {
                cfg.AddProfile<EntityMappingProfile>();
                cfg.AddProfile<ModelMappingProfile>();
            });

            return config.CreateMapper();
        }

        #endregion
    }
}
