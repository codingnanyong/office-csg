using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Infrastructure
{
    /// <summary>
    /// A disposable object.
    /// </summary>
    public interface IDisposableExtension : IDisposable
    {
        /// <summary>
        /// Gets a value indicating whether the object has been disposed.
        /// </summary>
        bool IsDisposed { get; }
    }
}
