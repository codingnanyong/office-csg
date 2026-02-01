using System;
using CSG.MI.TrMontrgSrv.EF;
using Microsoft.EntityFrameworkCore;

namespace CSG.MI.TrMontrgSrv.SL.RepoServices
{
    public class Repo<T> where T: class, IDisposable
    {
        #region Fields

        private readonly DbContextOptions<AppDbContext> _options;
        private AppDbContext _context;

        #endregion

        #region Constructors

        public Repo(DbContextOptions<AppDbContext> options)
        {
            _options = options;
        }

        #endregion

        #region Properties

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

        /// <summary>
        /// Create a new DbContext
        /// </summary>
        /// <returns></returns>
        public AppDbContext CreateContext() => new (_options);

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
