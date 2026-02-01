using System;
using System.Collections;
using System.Collections.Generic;
using System.Xml.Serialization;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Infrastructure
{
    public abstract class DisposableObject : IDisposableExtension
    {
        #region Properties

        /// <summary>
        /// Gets a value indicating whether the object has been disposed.
        /// </summary>
        [XmlIgnore]
        public virtual bool IsDisposed { get; private set; }

        #endregion

        #region Disposal

        /// <summary>
        /// Releases all resources currently held by the object.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases all resources currently held by the object.
        /// </summary>
        /// <param name="disposing"><see langword="True"/> if managed objects should be disposed, otherwise <see langword="false"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            IsDisposed = true;
        }

        /// <summary>
        /// Releases unmanaged resources and performs other cleanup operations before the
        /// object is reclaimed by garbage collection.
        /// </summary>
        ~DisposableObject()
        {
            Dispose(false);
        }

        #endregion

        #region Protected Methods

        /// <summary>
        /// Disposes the specified member if it implements <see cref="IDisposable"/>.
        /// </summary>
        /// <param name="member">The member to dispose.</param>
        protected static void DisposeMember(object member)
        {
            if (member is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        /// <summary>
        /// Disposes the collection and all of its contents, if they implement <see cref="IDisposable"/>.
        /// </summary>
        /// <param name="collection">The collection to dispose.</param>
        protected static void DisposeCollection(IEnumerable collection)
        {
            if (collection != null)
            {
                if (collection is IDisposableExtension)
                {
                    if ((collection as IDisposableExtension).IsDisposed == false)
                        foreach (object obj in collection)
                            DisposeMember(obj);
                }
                else
                {
                    foreach (object obj in collection)
                        DisposeMember(obj);
                }


                DisposeMember(collection);
            }
        }

        /// <summary>
        /// Disposes the dictionary and all of its contents, if they implement <see cref="IDisposable"/>.
        /// </summary>
        /// <param name="dictionary">The dictionary to dispose.</param>
        protected static void DisposeDictionary<K, V>(IDictionary<K, V> dictionary)
        {
            if (dictionary != null)
            {
                foreach (KeyValuePair<K, V> entry in dictionary)
                {
                    DisposeMember(entry.Key);
                    DisposeMember(entry.Value);
                }

                DisposeMember(dictionary);
            }
        }

        #endregion
    }
}
