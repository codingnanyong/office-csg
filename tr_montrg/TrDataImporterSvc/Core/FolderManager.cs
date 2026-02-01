using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.TrDataImporterSvc.Models;

namespace CSG.MI.TrMontrgSrv.TrDataImporterSvc.Core
{
    /// <summary>
    /// 폴더정보 저장관리를 위한 클래스.
    /// </summary>
    public class FolderManager
    {
        private static readonly string DIR_SEARCH_PATTERN = AppSettings.WatcherCfg.DirSearchPattern;
        private static readonly string FILE_FILTER_PATTERN = AppSettings.WatcherCfg.FileFilterPattern;
        private static readonly string ROOT_DIR = AppSettings.WatcherCfg.RootDir;
        private static readonly int SUBDIR_DEPTH = AppSettings.WatcherCfg.SubdirDepth;

        /// <summary>
        /// 설정파일로부터 폴더목록을 반환한다.
        /// </summary>
        /// <returns>파일 변화를 모니터 할 폴더 목록을 반환한다.</returns>
        public static List<Folder> LoadWatchingFolders()
        {
            var folders = new List<Folder>();   // 폴더 목록

            var root = new DirectoryInfo(ROOT_DIR);
            if (root.Exists == false)
                return folders;

            List<DirectoryInfo> dirs = GetDirectoriesInDepth(root, DIR_SEARCH_PATTERN, SUBDIR_DEPTH);
            dirs.ForEach(x =>
            {
                folders.Add(new Folder
                {
                    FullPath = x.FullName,
                    Filter = FILE_FILTER_PATTERN
                });
            });

            return folders;
        }


        /// <summary>
        /// Recursively search folders in depth of subfolders
        /// </summary>
        /// <param name="root">Root folder to search</param>
        /// <param name="searchPattern">Pattern of folder name to search</param>
        /// <param name="depth">Depth of subfolder</param>
        /// <returns>List of folders found</returns>
        private static List<DirectoryInfo> GetDirectoriesInDepth(DirectoryInfo root, string searchPattern, int depth)
        {
            List<DirectoryInfo> list = new();

            int rootDepth = root.FullName.Split("\\").Length;

            string[] allFullPath = Directory.GetDirectories(root.FullName, searchPattern, SearchOption.AllDirectories);
            HashSet<string> hashset = new();

            foreach (string fullPath in allFullPath)
            {
                string[] splitted = fullPath.Split("\\");

                if (splitted.Length >= rootDepth + depth)
                {
                    string[] rootToNth = splitted.Skip(rootDepth).Take(depth).ToArray();
                    string path = Path.Combine(root.FullName, String.Join("\\", rootToNth));
                    hashset.Add(path);
                }
            }

            hashset.ToList().ForEach(x => list.Add(new DirectoryInfo(x)));

            return list;
        }
    }
}
