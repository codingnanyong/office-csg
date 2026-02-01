//===============================================================================
// Copyright © Changshin Inc. All rights reserved.
//===============================================================================
// 개발자: jeawon.lee
// 개발일: 2021.02.17
// 수정내역:
// - 2021.02.17 jeawon.lee : Initial Version
//===============================================================================

using ICSharpCode.SharpZipLib.Core;
using ICSharpCode.SharpZipLib.Zip;
using System;
using System.IO;

namespace CSG.MI.AutoETL.Utils
{
    /// <summary>
    /// https://github.com/icsharpcode/SharpZipLib
    /// https://github.com/icsharpcode/SharpZipLib/wiki/Zip-Samples#anchorUnpackFull
    /// </summary>
    public class ZipHelper
    {
        /// <summary>
        ///
        /// </summary>
        /// <param name="file"></param>
        /// <param name="zipFile"></param>
        /// <param name="password"></param>
        /// <param name="compressionLevel"></param>
        /// <returns></returns>
        public static bool Zip(string file, string zipFile, string password = null, int compressionLevel = 6)
        {
            return Zip(new string[] { file }, zipFile, password, compressionLevel);
        }


        /// <summary>
        ///
        /// </summary>
        /// <param name="files"></param>
        /// <param name="zipFile"></param>
        /// <param name="password"></param>
        /// <param name="compressionLevel"></param>
        /// <returns></returns>
        public static bool Zip(string[] files, string zipFile, string password = null, int compressionLevel = 6)
        {
            using (var writer = File.Create(zipFile))
            using (var stream = new ZipOutputStream(writer))
            {
                // 0-9, 9 being the highest level of compression
                stream.SetLevel(compressionLevel);
                // optional.Null is the same as not setting.Required if using AES.
                stream.Password = password;

                foreach (var file in files)
                {
                    var f = new FileInfo(file);

                    var entryName = Path.GetFileName(f.FullName);
                    entryName = ZipEntry.CleanName(entryName);

                    var entry = new ZipEntry(entryName)
                    {
                        DateTime = f.LastWriteTime,
                        Size = f.Length
                    };

                    stream.PutNextEntry(entry);

                    using (var reader = File.OpenRead(file))
                    {
                        var buffer = new byte[4096];
                        StreamUtils.Copy(reader, stream, buffer);
                    }

                    stream.CloseEntry();
                }
            }

            return true;
        }

        /// <summary>
        /// Compress a single file with specified content, and create a zip file.
        /// </summary>
        /// <param name="content"></param>
        /// <param name="contentFile"></param>
        /// <param name="zipFile"></param>
        /// <param name="password"></param>
        /// <param name="compressionLevel"></param>
        /// <returns></returns>
        public static bool ZipFromContent(string content, string contentFile, string zipFile,
                                          string password = null, int compressionLevel = 6)
        {
            using (var fileStream = File.Create(zipFile))
            using (var zipStream = new ZipOutputStream(fileStream))
            {
                // 0-9, 9 being the highest level of compression
                zipStream.SetLevel(compressionLevel);
                // optional.Null is the same as not setting.Required if using AES.
                zipStream.Password = password;
                zipStream.PutNextEntry(new ZipEntry(contentFile));

                using (var writer = new StreamWriter(zipStream))
                {
                    writer.Write(content);
                }
            }

            return true;
        }

        /// <summary>
        /// Method for decompressing a compressed archive
        /// </summary>
        /// <param name="zipFile">the archive file we're decompressing</param>
        /// <param name="destPath">directory we want it unzipped to</param>
        /// <param name="deleteOriginal">delete or keep the original zip file</param>
        /// <returns></returns>
        public static bool Unzip(string zipFile, string destPath, bool deleteOriginal = false)
        {
            if (File.Exists(zipFile) == false)
            {
                return false;
            }

            using (var zipStream = new ZipInputStream(File.OpenRead(zipFile)))
            {
                ZipEntry entry;
                while ((entry = zipStream.GetNextEntry()) != null)
                {
                    string directoryName = Path.GetDirectoryName(entry.Name);
                    string fileName = Path.GetFileName(entry.Name);

                    // Create directory
                    // if (String.IsNullOrEmpty(directoryName) == false)
                    Directory.CreateDirectory(Path.Combine(destPath, directoryName));

                    if (String.IsNullOrEmpty(fileName) == false)
                    {
                        using (FileStream writer = File.Create(Path.Combine(destPath, entry.Name)))
                        {
                            int size = 2048;
                            byte[] data = new byte[size];
                            while (true)
                            {
                                size = zipStream.Read(data, 0, data.Length);
                                if (size > 0)
                                    writer.Write(data, 0, size);
                                else
                                    break;
                            }
                        }
                    }
                }
            }

            if (deleteOriginal == true)
                File.Delete(zipFile);

            return true;
        }

        /// <summary>
        /// Method for decompressing a compressed archive
        /// </summary>
        /// <param name="zipFile">the archive file we're decompressing</param>
        /// <param name="destPath">directory we want it unzipped to</param>
        /// <param name="password">archive password (if one exists; default is null)</param>
        /// <param name="deleteOriginal">delete or keep the original zip file</param>
        /// <returns></returns>
        public bool Unzip(string zipFile, string destPath, string password, bool deleteOriginal = false)
        {
            // Open a new ZipInputStream
            using (var zipStream = new ZipInputStream(File.OpenRead(zipFile)))
            {
                // Check for a password value, if none provided then set it to null (no password)
                if (String.IsNullOrEmpty(password) == false)
                    zipStream.Password = password;

                // Create a ZipEntry
                ZipEntry entry = null;
                string tempEntry = string.Empty;

                // Loop through the zip file grabbing each ZipEntry one at a time
                while ((entry = zipStream.GetNextEntry()) != null)
                {
                    string fileName = Path.GetFileName(entry.Name);

                    // Create the directory
                    if (String.IsNullOrEmpty(destPath) == false)
                        Directory.CreateDirectory(destPath);

                    // Make sure we have a file name and go from there
                    if (String.IsNullOrEmpty(fileName))
                        continue;

                    if (entry.Name.IndexOf(".ini") >= 0)
                        continue;

                    string path = destPath + @"\" + entry.Name;
                    path = path.Replace("\\ ", "\\");
                    string dirPath = Path.GetDirectoryName(path);

                    if (Directory.Exists(dirPath) == false)
                        Directory.CreateDirectory(dirPath);

                    using (FileStream stream = File.Create(path))
                    {
                        int size = 2048;
                        byte[] data = new byte[2048];
                        byte[] buffer = new byte[size];

                        while (true)
                        {
                            size = zipStream.Read(buffer, 0, buffer.Length);
                            if (size > 0)
                                // Write data to new file and go back and grab the next entry.
                                // Loop until all files have been decompressed and are in the proper directory
                                stream.Write(buffer, 0, size);
                            else
                                break;
                        }
                    }

                }
            }

            // One last thing to do, if the user provided true then we need to delete the original zip file,
            // otherwise we're now done.
            if (deleteOriginal == true)
                File.Delete(zipFile);

            return true;
        }

        /// <summary>
        /// Method for compressing a folder as a compressed archive
        /// </summary>
        /// <param name="path">destination folder to decompress the archive</param>
        /// <param name="zipFile">the archive file to compress</param>
        /// <returns></returns>
        public static bool ZipDirectory(string path, string zipFile)
        {
            return ZipDirectory(path, zipFile);
        }

        /// <summary>
        /// Method for compressing a folder as a compressed archive
        /// </summary>
        /// <param name="path">destination folder to decompress the archive</param>
        /// <param name="zipFile">the archive file to compress</param>
        /// <param name="compressionLevel">level of compression (default = 6)</param>
        /// <returns></returns>
        public static bool ZipDirectory(string path, string zipFile, int compressionLevel = 6)
        {
            return ZipDirectory(path, zipFile, null, compressionLevel);
        }

        /// <summary>
        /// Method for compressing a folder as a compressed archive
        /// </summary>
        /// <param name="path">destination folder to decompress the archive</param>
        /// <param name="zipFile">the archive file to compress</param>
        /// <param name="password">archive password (if neccessory; default = null)</param>
        /// <param name="compressionLevel">level of compression (default = 6)</param>
        /// <returns></returns>
        public static bool ZipDirectory(string path, string zipFile, string password = null, int compressionLevel = 6)
        {
            if (Directory.Exists(path) == false)
                return false;

            using (var zipStream = new ZipOutputStream(File.Create(zipFile)))
            {
                zipStream.SetLevel(compressionLevel); // 0-9, 9 being the highest level of compression

                if (String.IsNullOrEmpty(password) == false)
                    zipStream.Password = password;  // optional. Null is the same as not setting.

                // This setting will strip the leading part of the folder path in the entries, to
                // make the entries relative to the starting folder.
                // To include the full path for each entry up to the drive root, assign folderOffset = 0.
                int folderOffset = path.Length + (path.EndsWith("\\") ? 0 : 1);

                CompressFolder(path, zipStream, folderOffset);

                zipStream.IsStreamOwner = true;

                // Finish/Close arent needed strictly as the using statement does this automatically

                // Finish is important to ensure trailing information for a Zip file is appended.  Without this
                // the created file would be invalid.
                zipStream.Finish();
                // Makes the Close also Close the underlying stream
                zipStream.Close();
            }

            return true;
        }

        private static void CompressFolder(string path, ZipOutputStream zipStream, int folderOffset)
        {
            string[] files = Directory.GetFiles(path);

            foreach (string file in files)
            {
                var f = new FileInfo(file);

                string entryName = file.Substring(folderOffset);
                // Makes the name in zip based on the folder
                entryName = ZipEntry.CleanName(entryName);
                // Removes drive from name and fixes slash direction
                var entry = new ZipEntry(entryName);
                entry.DateTime = f.LastWriteTime;
                // Note the zip format stores 2 second granularity

                // Specifying the AESKeySize triggers AES encryption. Allowable values are 0 (off), 128 or 256.
                //   newEntry.AESKeySize = 256;

                // To permit the zip to be unpacked by built-in extractor in WinXP and Server2003, WinZip 8, Java, and other older code,
                // you need to do one of the following: Specify UseZip64.Off, or set the Size.
                // If the file may be bigger than 4GB, or you do not need WinXP built-in compatibility, you do not need either,
                // but the zip will be in Zip64 format which not all utilities can understand.
                //   zipStream.UseZip64 = UseZip64.Off;
                entry.Size = f.Length;

                zipStream.PutNextEntry(entry);

                // Zip the file in buffered chunks
                // the "using" will close the stream even if an exception occurs
                byte[] buffer = new byte[4096];
                using (var streamReader = File.OpenRead(file))
                {
                    StreamUtils.Copy(streamReader, zipStream, buffer);
                }
                zipStream.CloseEntry();
            }

            string[] folders = Directory.GetDirectories(path);
            foreach (string folder in folders)
            {
                CompressFolder(folder, zipStream, folderOffset);
            }
        }
    }
}
