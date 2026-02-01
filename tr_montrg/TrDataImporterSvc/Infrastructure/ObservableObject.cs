using System;
using System.ComponentModel;
using System.Diagnostics;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Infrastructure
{
    public abstract class ObservableObject : IObservableObject
    {
        #region Debugging Aides

        /// <summary>
        /// Warns the developer if this object does not have
        /// a public property with the specified name. This
        /// method does not exist in a Release build.
        /// </summary>
        [Conditional("DEBUG")]
        [DebuggerStepThrough]
        public void VerifyPropertyName(string propertyName)
        {
            // Verify that the property name matches a real,
            // public, instance property on this object.
            if (TypeDescriptor.GetProperties(this)[propertyName] == null)
            {
                string msg = "Invalid property name: " + propertyName;

                if (this.ThrowOnInvalidPropertyName)
                    throw new Exception(msg);
                else
                    Debug.Fail(msg);
            }
        }

        /// <summary>
        /// Returns whether an exception is thrown, or if a Debug.Fail() is used
        /// when an invalid property name is passed to the VerifyPropertyName method.
        /// The default value is false, but subclasses used by unit tests might
        /// override this property's getter to return true.
        /// </summary>
        protected virtual bool ThrowOnInvalidPropertyName { get; private set; }

        #endregion // Debugging Aides

        #region INotifyPropertyChanging implementation

        public event PropertyChangingEventHandler PropertyChanging;

        protected virtual void OnPropertyChanging(object sender, string propertyName)
        {
            this.VerifyPropertyName(propertyName);

            this.PropertyChanging?.Invoke(sender, new PropertyChangingEventArgs(propertyName));
        }

        #endregion

        #region INotifyPropertyChanged implementation

        public event PropertyChangedEventHandler PropertyChanged;

        protected virtual void OnPropertyChanged(object sender, string propertyName)
        {
            this.VerifyPropertyName(propertyName);

            this.PropertyChanged?.Invoke(sender, new PropertyChangedEventArgs(propertyName));
        }

        #endregion
    }
}
