using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Helpers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

namespace CSG.MI.TrMontrgSrv.EF.Test.Fixtures
{
    public class GenericFixture<T> where T : class, IDisposable
    {
        #region Fields

        //private const string CONN_STRING_FILE = "appsettings.json";
        private AppDbContext _context;

        #endregion

        #region Constructors

        public GenericFixture()
        {
            //var config = new ConfigurationBuilder()
            //                .AddJsonFile(CONN_STRING_FILE)
            //                .Build();
            var config = ConfigurationBuilderSingleton.ConfigurationRoot;

            var connString = config["Data:TrMontrgSrv:ConnectionString"];
            var builder = new DbContextOptionsBuilder<AppDbContext>()
                                .UseNpgsql(connString);
            //.UseInMemoryDatabase(databaseName: "tr_montrg_srv");
            var options = builder.Options;

            Config = config;
            ConnString = connString;
            Builder = builder;
            Options = options;
        }

        #endregion

        #region Properties

        public IConfigurationRoot Config { get; private set; }

        public string ConnString { get; private set; }

        public DbContextOptionsBuilder<AppDbContext> Builder { get; private set; }

        public DbContextOptions<AppDbContext> Options { get; private set; }

        public AppDbContext Context
        {
            get
            {
                if (_context == null)
                    _context = CreateContext();

                return _context;
            }
            set
            {
                _context = value;
            }
        }

        protected bool IsDisposed { get; private set; }

        #endregion

        #region Public Methods

        public AppDbContext CreateContext() => new (Options);

        /// <summary>
        /// Create new repository with new DbContext
        /// </summary>
        /// <returns>Device repository</returns>
        public T CreateRepo() => (T)Activator.CreateInstance(typeof(T), CreateContext());

        #endregion

        #region IDisposable Members

        protected virtual void Dispose(bool disposing)
        {
            if (IsDisposed)
                return;

            if (disposing)
            {
                // TODO: dispose managed state (managed objects)
                Context?.Database?.CloseConnection();
                Context?.Dispose();
            }

            // TODO: free unmanaged resources (unmanaged objects) and override finalizer
            // TODO: set large fields to null
            Config = null;
            ConnString = null;
            Builder = null;
            Options = null;
            Context = null;

            IsDisposed = true;
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            //GC.SuppressFinalize(this);
        }

        #endregion
    }
}
