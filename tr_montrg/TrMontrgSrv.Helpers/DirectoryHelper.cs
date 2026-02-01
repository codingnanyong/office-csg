using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Helpers
{
    public static class DirectoryHelper
    {
        #region Fields

        private static readonly char[] INVALID_DIR_NAME_CHARS = new char[] { '\\', '/', ':', '*', '?', '"', '<', '>', '|' };

        #endregion

        #region Public Methods

        /// <summary>
        /// Returns invalid characters for directory name on NTFS.
        /// </summary>
        public static char[] InvalidDirectoryNameCharacters
        {
            get { return INVALID_DIR_NAME_CHARS; }
        }

        /// <summary>
        /// Checks whether the specified path exists.
        /// </summary>
        /// <param name="path">Path to check existance</param>
        /// <returns></returns>
        public static bool Exists(string path)
        {
            if (String.IsNullOrEmpty(path) == true)
                return false;

            bool exist;
            try
            {
                exist = Directory.Exists(path);
            }
            catch
            {
                exist = false;
            }

            return exist;
        }

        public static DirectoryInfo[] GetSubDirs(string path)
        {
            if (Exists(path) == false)
                return Array.Empty<DirectoryInfo>();

            var dir = new DirectoryInfo(path);

            return dir.GetDirectories();
        }

        /// <summary>
        /// Copy folder to another folder recursively
        /// </summary>
        /// <param name="sourceFolder"></param>
        /// <param name="destFolder"></param>
        /// <param name="overwrite"></param>
        public static void Copy(string sourceFolder, string destFolder, bool overwrite = false)
        {
            CreateIfNotExist(destFolder);

            IEnumerable<string> files = Directory.EnumerateFiles(sourceFolder);
            IEnumerable<string> folders = Directory.EnumerateDirectories(sourceFolder);

            foreach (string file in files)
            {
                string name = Path.GetFileName(file);
                string dest = Path.Combine(destFolder, name);
                File.Copy(file, dest, overwrite);
            }

            foreach (string folder in folders)
            {
                string name = Path.GetFileName(folder);
                string dest = Path.Combine(destFolder, name);
                Copy(folder, dest, overwrite);
            }
        }

        /// <summary>
        /// Move folder to another folder recursively
        /// </summary>
        /// <param name="sourceFolder"></param>
        /// <param name="destFolder"></param>
        /// <param name="overwrite"></param>
        public static void Move(string sourceFolder, string destFolder, bool overwrite = false)
        {
            Copy(sourceFolder, destFolder, overwrite);
            Delete(sourceFolder);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="path"></param>
        /// <remarks>
        /// Directory.Delete(path, true)
        /// Beware of this approach if your directory your deleting has shortcuts/symbolic links to other folders - you may end up deleting more then you expected
        /// </remarks>
        public static void Delete(string path)
        {
            IEnumerable<string> files = Directory.EnumerateFiles(path);
            IEnumerable<string> dirs = Directory.EnumerateDirectories(path);

            foreach (string file in files)
            {
                File.SetAttributes(file, FileAttributes.Normal);
                File.Delete(file);
            }

            foreach (string dir in dirs)
            {
                Delete(dir);
            }

            Directory.Delete(path, false);
        }

        /// <summary>
        /// Creates a directory if it doesn't exist.
        /// </summary>
        /// <param name="path">Full path of a directory to create</param>
        /// <returns><c>true</c> if created, otherwise <c>false</c></returns>
        public static DirectoryInfo CreateIfNotExist(string path)
        {
            if (Exists(path) == false)
            {
                return Directory.CreateDirectory(path);
            }

            return null;
        }

        /// <summary>
        /// Creates a directory.
        /// </summary>
        /// <param name="path">Directory path to create</param>
        /// <returns><c>true</c> if created, otherwise <c>false</c></returns>
        public static bool Create(string path)
        {
            try
            {
                DirectoryInfo dir = CreateIfNotExist(path);
                if (dir == null)
                    return false;
            }
            catch
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// Deletes a directory if it exists.
        /// </summary>
        /// <param name="path">Full path of a directory</param>
        public static void DeleteIfExist(string path)
        {
            if (Exists(path))
                Directory.Delete(path);
        }

        /// <summary>
        /// Determine whether the specified dirtectory is empty or not.
        /// </summary>
        /// <param name="path">Dirtectory path to determine</param>
        /// <returns>True if empty, otherwise false</returns>
        public static bool IsDirectoryEmpty(string path)
        {
            return !Directory.EnumerateFileSystemEntries(path).Any();
        }

        #endregion // Public Methods
    }
}
