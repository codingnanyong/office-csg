using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace CSG.MI.TrMontrgSrv.Helpers
{
    public static class FileHelper
    {
        /// <summary>
        /// Type of access right
        /// </summary>
        public enum AccessRight { None, Read, Write, ChangeWrite, Full }

        /// <summary>
        /// Returns invalid characters for file name on NTFS.
        /// </summary>
        public static char[] InvalidFileNameCharacters
        {
            get { return DirectoryHelper.InvalidDirectoryNameCharacters; }
        }

        /// <summary>
        /// Tries to get file stream with time interval.
        /// </summary>
        /// <param name="fileName">File name and path to get</param>
        /// <param name="fileAccess">Type of file access</param>
        /// <param name="numberOfTrials">Number of trials</param>
        /// <param name="timeIntervalBetweenTrials">Time interval between trials</param>
        /// <returns>File stream</returns>
        public static FileStream GetStream(string fileName, FileAccess fileAccess, int numberOfTrials, int timeIntervalBetweenTrials)
        {
            var trialCount = 0;

            while (true)
            {
                try
                {
                    return File.Open(fileName, FileMode.OpenOrCreate, fileAccess, FileShare.None);
                }
                catch (IOException e)
                {
                    if (IsFileLocked(e) == false)
                        throw;

                    if (++trialCount > numberOfTrials)
                        throw new Exception("The file is locked too long time: " + e.Message, e);

                    Thread.Sleep(timeIntervalBetweenTrials);
                }
            }
        }

        /// <summary>
        /// Indicates whether the specified filter pattern finds a match in the specifed input string, using the specified matching options.
        /// </summary>
        /// <param name="fileName">The string to search for a match.</param>
        /// <param name="filterPattern">Filter pattern to match such as "*.csv".</param>
        /// <param name="options">A bitwise combination of the enumaration values that provide options for matching.</param>
        /// <returns>true if the pattern finds a match; otherwise, false</returns>
        /// <example>
        /// bool isMatched = FileHelper.IsMatch("MyFileName.txt", "*.txt");
        /// isMatched = FileHelper.IsMatch("MyFileName.txt", "myfilename.txt", RegexOptions.IgnoreCase);
        /// </example>
        public static bool IsFileNameMatch(string fileName, string filterPattern, RegexOptions options = RegexOptions.None)
        {
            var filter = filterPattern;
            foreach (char x in @"\+?|{[()^$.#")
            {
                filter = filter.Replace(x.ToString(), @"\" + x.ToString());
            }
            var rgx = new Regex(String.Format("^{0}$", filter.Replace("*", ".*")), options);

            return rgx.IsMatch(fileName);
        }

        #region Existence

        public static bool Exists(string path)
        {
            if (String.IsNullOrEmpty(path) == true)
                return false;

            bool exist;
            try
            {
                exist = File.Exists(path);
            }
            catch
            {
                exist = false;
            }

            return exist;
        }

        /// <summary>
        /// Checks whether the specified file exists in the path
        /// </summary>
        /// <param name="path">File path to check</param>
        /// <param name="totalTrials">Total number of trials to check the existence</param>
        /// <param name="timeIntervalBetweenTrials">Time interval in millisecond between trials</param>
        /// <returns>Ture if the file found, otherwise false</returns>
        public static bool Exists(string path, int totalTrials, int timeIntervalBetweenTrials)
        {
            if (Exists(path) == true)
                return true;

            int i = totalTrials;

            while (--i > 0)
            {
                Thread.Sleep(timeIntervalBetweenTrials);

                if (Exists(path) == true)
                    return true;
            }

            return false;
        }

        #endregion

        #region Check

        /// <summary>
        /// Checks whether the specified file is writable.
        /// </summary>
        /// <param name="fileName">File path</param>
        /// <returns></returns>
        public static bool IsWritable(string fileName)
        {
            FileStream fs = null;

            try
            {
                fs = new FileStream(fileName, FileMode.OpenOrCreate);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
            finally
            {
                if (fs != null)
                {
                    fs.Close();
                    fs.Dispose();
                }
            }
        }

        /// <summary>
        /// Determines whether a specified file is locked.
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public static bool IsFileLocked(string filePath)
        {
            FileStream fs = null;

            try
            {
                fs = File.Open(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException e)
            {
                if (IsFileLocked(e) == true)
                    return true;

                return false;
            }
            finally
            {
                if (fs != null)
                {
                    fs.Close();
                    fs.Dispose();
                }
            }

            return false;
        }

        private static bool IsFileLocked(IOException exception)
        {
            int errorCode = Marshal.GetHRForException(exception) & ((1 << 16) - 1);

            return errorCode == 32 || errorCode == 33;
        }

        #endregion // Check

        #region Change Attributes

        /// <summary>
        /// Remove a single ReadOnly attribute from a specified file.
        /// </summary>
        /// <param name="filePath">Path of a file to remove the ReadOnly attribute</param>
        /// <exception cref="ArgumentNullException">The <paramref name="filePath"/> is <c>null</c>.</exception>
        public static void RemoveReadOnlyAttribute(string filePath)
        {
            FileAttributes fa = File.GetAttributes(filePath);
            if ((fa & FileAttributes.ReadOnly) > 0)
            {
                fa ^= FileAttributes.ReadOnly;
                File.SetAttributes(filePath, fa);
            }
        }

        #endregion

        #region Delete

        /// <summary>
        ///
        /// </summary>
        /// <param name="directory"></param>
        /// <param name="filePattern"></param>
        /// <returns></returns>
        public static int DeleteFiles(string directory, string filePattern)
        {
            int totalDeleted = 0;

            IEnumerable<string> files = Directory.EnumerateFiles(directory, filePattern);
            foreach (string file in files)
            {
                try
                {
                    File.SetAttributes(file, FileAttributes.Normal);
                    File.Delete(file);
                    totalDeleted++;
                }
                catch
                {
                    // Intentional silent catching
                }
            }

            return totalDeleted;
        }
        /// <summary>
        /// 지정한 파일을 삭제한다.
        /// </summary>
        /// <param name="path">파일 경로</param>
        /// <returns></returns>
        public static bool DeleteFile(string path)
        {
            if (Exists(path) == false)
                return false;

            try
            {
                File.Delete(path);
            }
            catch
            {
                return false;
            }

            return !Exists(path);
        }

        /// <summary>
        /// Delete all files in a specified directory that match a file pattern and that have existed
        /// longer than a specified number of minutes.
        /// </summary>
        /// <param name="directory">The directory to delete files from</param>
        /// <param name="filePattern">The file pattern</param>
        /// <param name="expirationMinutes">Minutes for expiration time</param>
        public static int DeleteExpiredFiles(string directory, string filePattern, int expirationMinutes)
        {
            int totalDeleted = 0;

            IEnumerable<string> files = Directory.EnumerateFiles(directory, filePattern);
            foreach (string file in files)
            {
                FileInfo fileInfo = new(file);
                // Delete files older than the specifed expirtation minute ago
                if (fileInfo.CreationTime.CompareTo(DateTime.Now.Subtract(new TimeSpan(0, expirationMinutes, 0))) < 0)
                {
                    try
                    {
                        File.SetAttributes(file, FileAttributes.Normal);
                        File.Delete(file);
                        totalDeleted++;
                    }
                    catch
                    {
                        // Intentional silent catching
                    }
                }
            }

            return totalDeleted;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="path"></param>
        /// <param name="filePattern"></param>
        /// <param name="expirationDays"></param>
        public static void DeleteExpiredFilesRecursively(string path, string filePattern, int expirationDays)
        {
            IEnumerable<string> files = Directory.EnumerateFiles(path, filePattern);
            IEnumerable<string> dirs = Directory.EnumerateDirectories(path);

            foreach (string file in files)
            {
                FileInfo fileInfo = new(file);
                // Delete files older than the specifed expirtation day ago
                if (fileInfo.CreationTime.CompareTo(DateTime.Now.Subtract(new TimeSpan(expirationDays * 24, 0, 0))) < 0)
                {
                    try
                    {
                        File.SetAttributes(file, FileAttributes.Normal);
                        File.Delete(file);
                    }
                    catch
                    {
                        // Intentional silent catching
                    }
                }
            }

            foreach (string dir in dirs)
            {
                DeleteExpiredFilesRecursively(dir, filePattern, expirationDays);
            }
        }

        #endregion

        #region Find

        /// <summary>
        /// Recursively list all files in a directory
        /// </summary>
        /// <param name="path">Target path to search</param>
        /// <param name="pattern">Search pattern in a specified path</param>
        /// <returns>List of file names</returns>
        public static List<string> FindFiles(string path, string pattern)
        {
            var list = new List<string>();

            var files = Directory.EnumerateFiles(path, pattern, SearchOption.AllDirectories);
            foreach (var file in files)
            {
                list.Add(file);
            }

            return list;
        }

        /// <summary>
        /// Indicate whether a specified path contains any files or not
        /// </summary>
        /// <param name="path">Target path to inspect</param>
        /// <param name="pattern">Search pattern in a specified path</param>
        /// <returns>True if any file found, otherwise false</returns>
        public static bool ContainAnyFile(string path, string pattern)
        {
            var files = FileHelper.FindFiles(path, pattern);

            return files.Count != 0;
        }

        #endregion

        #region Write

        /// <summary>
        /// "filename"파일이 있으면 열어서 맨 뒤에 content를 쓴다,
        /// "filename"파일이 없으면 생성 후 content를 쓴다.
        /// </summary>
        /// <param name="fileName">파일 경로</param>
        /// <param name="content">쓰일 내용</param>
        /// <param name="encoding">인코딩(기본값: Encoding.Default)</param>
        /// <returns></returns>
        public static bool WriteLastString(string fileName, string content, Encoding encoding = null)
        {
            try
            {
                if (IsWritable(fileName) == false)
                    return false;

                var encodingType = encoding ?? Encoding.Default;

                using StreamWriter stream = new(fileName, true, encodingType);
                stream.WriteLine(content);
            }
            catch
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// 지정한 경로에 파일을 생성한다. append에 값에 따라서 새로생성할지/뒤에 붙일지를 판단한다.
        /// </summary>
        /// <param name="path">파일 경루</param>
        /// <param name="content">텍스트 내용</param>
        /// <param name="append">Append 유무</param>
        /// <param name="encoding">인코딩(기본값: Encoding.UTF8)</param>
        /// <returns></returns>
        public static bool CreateTextFile(string path, string content, bool append, Encoding encoding = null)
        {
            if (String.IsNullOrEmpty(path) == true)
                return false;

            var encodingType = encoding ?? Encoding.UTF8;

            try
            {
                using StreamWriter sw = new(path, append, encodingType);
                sw.Write(content);
            }
            catch
            {
                return false;
            }

            return true;
        }

        /// <summary>
        /// 저장한 파일을 text로 읽어서 반환한다.
        /// </summary>
        /// <param name="path">파일 경로</param>
        /// <param name="encoding">인코딩(기본값: Encoding.UTF8)</param>
        /// <returns></returns>
        public static string ReadTextFile(string path, Encoding encoding = null)
        {
            string content = null;
            var encodingType = encoding ?? Encoding.UTF8;

            if (String.IsNullOrEmpty(path) == true)
                return null;

            try
            {
                using StreamReader sr = new(path, encodingType);
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    content += (line + Environment.NewLine);
                }
            }
            catch
            {
                return null;
            }

            return content;
        }

        #endregion

        #region Move

        public static void Move(string sourceFileName, string destFileName, bool overwrite = false)
        {
            if (overwrite)
                FileHelper.DeleteFile(destFileName);

            File.Move(sourceFileName, destFileName);
        }

        #endregion
    }
}
