using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CSG.MI.TrMontrgSrv.Model;

namespace CSG.MI.TrMontrgSrv.BLL
{
    public class Util
    {
        public static void ValidateArgs(string ymd, string hms, string deviceId, int? id = null)
        {
            if (String.IsNullOrWhiteSpace(ymd) || String.IsNullOrWhiteSpace(hms) || String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException:null);

            if ((ymd + hms).ToDateTime() == null)
                throw new ArgumentException("Failed to convert YMDHMS string to DateTime.", innerException:null);

            if (id.HasValue && id.Value < 0)
                throw new ArgumentException("Integer ID cannot be less than zero.", innerException: null);
        }

        public static void ValidateArgs(string deviceId, int? id = null)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            if (id.HasValue && id.Value < 0)
                throw new ArgumentException("Integer ID cannot be less than zero.", innerException: null);
        }

        public static void ValidateArgs(DateTime start, DateTime end, string deviceId, int? id = null)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            if (start > end)
                throw new ArgumentException("Start time connot be later than end time.", innerException: null);

            if (id.HasValue && id.Value < 0)
                throw new ArgumentException("Integer ID cannot be less than zero.", innerException: null);
        }

        public static void ValidateArgs(string startYmd, string startHms, string endYmd, string endHms, string deviceId, int? id = null)
        {
            if (String.IsNullOrWhiteSpace(deviceId))
                throw new ArgumentNullException("Augument cannot be empty or null.", innerException: null);

            var startTime = (startYmd + startHms).ToDateTime();
            var endTime = (endYmd + endHms).ToDateTime();
            if (startTime == null || endTime == null)
                throw new ArgumentException("Failed to convert YMDHMS string to DateTime.", innerException: null);

            if (startTime > endTime)
                throw new ArgumentException("Start time connot be later than end time.", innerException: null);

            if (id.HasValue && id.Value < 0)
                throw new ArgumentException("Integer ID cannot be less than zero.", innerException: null);
        }





    }
}
