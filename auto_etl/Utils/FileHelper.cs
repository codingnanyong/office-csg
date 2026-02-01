//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace CSG.MI.AutoETL.Utils
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

            bool exist = false;

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
            catch
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

        #region WhoIsLocking
        // http://stackoverflow.com/questions/317071/how-do-i-find-out-which-process-is-locking-a-file-using-net/20623311#20623311

        [StructLayout(LayoutKind.Sequential)]
        struct RM_UNIQUE_PROCESS
        {
            public int dwProcessId;
            public System.Runtime.InteropServices.ComTypes.FILETIME ProcessStartTime;
        }

        const int RmRebootReasonNone = 0;
        const int CCH_RM_MAX_APP_NAME = 255;
        const int CCH_RM_MAX_SVC_NAME = 63;

        enum RM_APP_TYPE
        {
            RmUnknownApp = 0,
            RmMainWindow = 1,
            RmOtherWindow = 2,
            RmService = 3,
            RmExplorer = 4,
            RmConsole = 5,
            RmCritical = 1000
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        struct RM_PROCESS_INFO
        {
            public RM_UNIQUE_PROCESS Process;

            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = CCH_RM_MAX_APP_NAME + 1)]
            public string strAppName;

            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = CCH_RM_MAX_SVC_NAME + 1)]
            public string strServiceShortName;

            public RM_APP_TYPE ApplicationType;
            public uint AppStatus;
            public uint TSSessionId;
            [MarshalAs(UnmanagedType.Bool)]
            public bool bRestartable;
        }

        [DllImport("rstrtmgr.dll", CharSet = CharSet.Unicode)]
        static extern int RmRegisterResources(uint pSessionHandle,
                                                          UInt32 nFiles,
                                                          string[] rgsFilenames,
                                                          UInt32 nApplications,
                                                          [In] RM_UNIQUE_PROCESS[] rgApplications,
                                                          UInt32 nServices,
                                                          string[] rgsServiceNames);

        [DllImport("rstrtmgr.dll", CharSet = CharSet.Auto)]
        static extern int RmStartSession(out uint pSessionHandle, int dwSessionFlags, string strSessionKey);

        [DllImport("rstrtmgr.dll")]
        static extern int RmEndSession(uint pSessionHandle);

        [DllImport("rstrtmgr.dll")]
        static extern int RmGetList(uint dwSessionHandle,
                                             out uint pnProcInfoNeeded,
                                             ref uint pnProcInfo,
                                             [In, Out] RM_PROCESS_INFO[] rgAffectedApps,
                                             ref uint lpdwRebootReasons);

        /// <summary>
        /// Find out what process(es) have a lock on the specified file.
        /// </summary>
        /// <param name="path">Path of the file.</param>
        /// <returns>Processes locking the file</returns>
        /// <remarks>See also:
        /// http://msdn.microsoft.com/en-us/library/windows/desktop/aa373661(v=vs.85).aspx
        /// http://wyupdate.googlecode.com/svn-history/r401/trunk/frmFilesInUse.cs (no copyright in code at time of viewing)
        /// </remarks>
        public static List<Process> WhoIsLocking(string path)
        {
            uint handle;
            string key = Guid.NewGuid().ToString();
            List<Process> processes = new List<Process>();

            int res = RmStartSession(out handle, 0, key);
            if (res != 0) throw new Exception("Could not begin restart session.  Unable to determine file locker.");

            try
            {
                const int ERROR_MORE_DATA = 234;
                uint pnProcInfoNeeded = 0,
                      pnProcInfo = 0,
                      lpdwRebootReasons = RmRebootReasonNone;

                string[] resources = new string[] { path }; // Just checking on one resource.

                res = RmRegisterResources(handle, (uint)resources.Length, resources, 0, null, 0, null);

                if (res != 0) throw new Exception("Could not register resource.");

                //Note: there's a race condition here -- the first call to RmGetList() returns
                //      the total number of process. However, when we call RmGetList() again to get
                //      the actual processes this number may have increased.
                res = RmGetList(handle, out pnProcInfoNeeded, ref pnProcInfo, null, ref lpdwRebootReasons);

                if (res == ERROR_MORE_DATA)
                {
                    // Create an array to store the process results
                    RM_PROCESS_INFO[] processInfo = new RM_PROCESS_INFO[pnProcInfoNeeded];
                    pnProcInfo = pnProcInfoNeeded;

                    // Get the list
                    res = RmGetList(handle, out pnProcInfoNeeded, ref pnProcInfo, processInfo, ref lpdwRebootReasons);
                    if (res == 0)
                    {
                        processes = new List<Process>((int)pnProcInfo);

                        // Enumerate all of the results and add them to the
                        // list to be returned
                        for (int i = 0; i < pnProcInfo; i++)
                        {
                            processes.Add(Process.GetProcessById(processInfo[i].Process.dwProcessId));
                        }
                    }
                    else throw new Exception("Could not list processes locking resource.");
                }
                else if (res != 0) throw new Exception("Could not list processes locking resource. Failed to get size of result.");
            }
            finally
            {
                RmEndSession(handle);
            }

            return processes;
        }

        #endregion // WhoIsLocking

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
                FileInfo fileInfo = new FileInfo(file);
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
                FileInfo fileInfo = new FileInfo(file);
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

            return (files.Count == 0) ? false : true;
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

                var encodingType = (encoding == null) ? Encoding.Default : encoding;

                using (StreamWriter stream = new StreamWriter(fileName, true, encodingType))
                {
                    stream.WriteLine(content);
                }
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

            var encodingType = (encoding == null) ? Encoding.UTF8 : encoding;

            try
            {
                using (StreamWriter sw = new StreamWriter(path, append, encodingType))
                {
                    sw.Write(content);
                }
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
            var encodingType = (encoding == null) ? Encoding.UTF8 : encoding;

            if (String.IsNullOrEmpty(path) == true)
                return null;

            try
            {
                using (StreamReader sr = new StreamReader(path, encodingType))
                {
                    string line;
                    while ((line = sr.ReadLine()) != null)
                    {
                        content += (line + Environment.NewLine);
                    }
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

        #region Copy
        public static void Copy(string sourceFileName, string destFileName, bool overwrite = false)
        {
            File.Copy(sourceFileName, destFileName, overwrite);
        }
        #endregion

        #region Change Extension

        public static string ChangeExtension(string path, string ext)
        {
            var dstPath = Path.ChangeExtension(path, ext); // Example: C:\path\to\file.tmp
            File.Move(path, dstPath); // 원본 파일 확장자 변경, Delete 하지 않아도 됨

            return dstPath;
        }

        #endregion
    }
}
