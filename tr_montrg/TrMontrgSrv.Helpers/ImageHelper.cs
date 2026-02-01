using System.Data;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.IO;

namespace CSG.MI.TrMontrgSrv.Helpers
{
    public abstract class ImageHelper
    {
        /// <summary>
        /// Opens a file in a filestream from a specified path
        /// and reads its data in a byte array.
        /// </summary>
        /// <param name="path">File path to read</param>
        /// <returns>Byte array</returns>
        public static byte[] ReadFile(string path)
        {
            byte[] data = null;

            // Use FileInfo object to get file size.
            FileInfo fileInfo = new (path);
            long size = fileInfo.Length;

            // Open FileStream to read file
            using (FileStream fs = new (path, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new (fs))
            {
                data = reader.ReadBytes((int)size);
            }

            return data;
        }

        /// <summary>
        /// Reads an image from a specified path.
        /// </summary>
        /// <param name="path">Image path to read</param>
        /// <returns>Image object</returns>
        public static Image ReadImageFile(string path)
        {
            if (File.Exists(path) == false)
                return null;

            return ByteArrayToImage(ReadFile(path));
        }


        /// <summary>
        /// Converts a byte array to a bitmap image
        /// </summary>
        /// <param name="byteArray">Byte array to convert</param>
        /// <returns>Bitmap image</returns>
        public static Bitmap BytesToBmp(byte[] byteArray)
        {
            MemoryStream ms = new(byteArray);
            return new (ms);
        }


        /// <summary>
        /// Coverts a byte array to an image object
        /// </summary>
        /// <param name="byteArray">Byte array to convert</param>
        /// <returns>Image object</returns>
        public static Image ByteArrayToImage(byte[] byteArray)
        {
            MemoryStream ms = new(byteArray, false);
            return Image.FromStream(ms, true);
        }


        /// <summary>
        /// Gets a byte array from a column of DataReader. The database column must be in binary format.
        /// </summary>
        /// <param name="dataReader">DataReader to fetch data</param>
        /// <param name="columnName">Name of column to read</param>
        /// <returns>A byte array</returns>
        public static byte[] GetByteArrayFromDataReader(IDataReader dataReader, string columnName)
        {
            return dataReader[columnName] as byte[];
        }

        /// <summary>
        /// Gets an Image object from a column of DataReader. The database column must be in binary format.
        /// </summary>
        /// <param name="dataReader">DataReader to fetch data</param>
        /// <param name="columnName">Name of column to read</param>
        /// <returns>A byte array</returns>
        public static Image GetImageFromDataReader(IDataReader dataReader, string columnName)
        {
            return ByteArrayToImage(GetByteArrayFromDataReader(dataReader, columnName));
        }

        /// <summary>
        /// Resize an image if width or height of image is larger than specified max value.
        /// </summary>
        /// <param name="path">Image path to read</param>
        /// <param name="maxWidth">Max. allowed width of image</param>
        /// <param name="maxHeight">Max. allowed height of image</param>
        /// <returns>System.Drawing.Bitmap</returns>
        public static Image ResizeIf(string path, int maxWidth, int maxHeight)
        {
            Bitmap bitmap = null;

            using (System.Drawing.Image img = System.Drawing.Image.FromFile(path))
            {
                int w = (maxWidth > 0) ? maxWidth : img.Width;
                int h = (maxHeight > 0) ? maxHeight : img.Height;
                double scaleWidth = (double)w / (double)img.Width;
                double scaleHeight = (double)h / (double)img.Height;
                double scale = (scaleHeight < scaleWidth) ? scaleHeight : scaleWidth;
                scale = (scale > 1) ? 1 : scale;

                int newWidth = (int)(scale * img.Width);
                int newHeight = (int)(scale * img.Height);

                bitmap = new Bitmap(newWidth, newHeight);

                using Graphics g = Graphics.FromImage(bitmap);
                g.CompositingQuality = CompositingQuality.HighQuality;
                g.SmoothingMode = SmoothingMode.HighQuality;
                g.InterpolationMode = InterpolationMode.HighQualityBicubic;
                g.PixelOffsetMode = PixelOffsetMode.HighQuality;
                g.DrawImage(img, new Rectangle(0, 0, newWidth, newHeight));
                g.Save();
            }

            return bitmap;
        }
    }
}
