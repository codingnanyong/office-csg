using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSG.MI.TrMontrgSrv.Model
{
    public static class Util
    {
        public static DateTime? ToDateTime(this string ymdhms)
        {
            if (String.IsNullOrEmpty(ymdhms))
                return null;

            DateTime? dt;
            int year, month, day, hour, minute, second;

            // Remove underscore(_) character
            ymdhms = ymdhms.Replace("_", "");

            try
            {
                switch (ymdhms.Length)
                {
                    case 14:
                        year = Convert.ToInt32(ymdhms.Substring(0, 4));
                        month = Convert.ToInt32(ymdhms.Substring(4, 2));
                        day = Convert.ToInt32(ymdhms.Substring(6, 2));
                        hour = Convert.ToInt32(ymdhms.Substring(8, 2));
                        minute = Convert.ToInt32(ymdhms.Substring(10, 2));
                        second = Convert.ToInt32(ymdhms.Substring(12, 2));
                        dt = new DateTime(year, month, day, hour, minute, second);
                        break;
                    case 8:
                        year = Convert.ToInt32(ymdhms.Substring(0, 4));
                        month = Convert.ToInt32(ymdhms.Substring(4, 2));
                        day = Convert.ToInt32(ymdhms.Substring(6, 2));
                        dt = new DateTime(year, month, day);
                        break;
                    default:
                        dt = null;
                        break;
                }
            }
            catch
            {
                dt = null;
            }

            return dt;
        }

        public static string ToYmdHms(this DateTime dt)
        {
            var tuple = dt.ToYmdHmsTuple();

            return $"{tuple.Item1}{tuple.Item2}";
        }

        public static string ToYmdHmsWithSeparator(this DateTime dt)
        {
            var tuple = dt.ToYmdHmsTuple();

            return $"{tuple.Item1}_{tuple.Item2}";
        }

        public static Tuple<string, string> ToYmdHmsTuple(this DateTime dt)
        {
            string year = dt.Year.ToString("D4");
            string month = dt.Month.ToString("D2");
            string day = dt.Day.ToString("D2");
            string hour = dt.Hour.ToString("D2");
            string minute = dt.Minute.ToString("D2");
            string second = dt.Second.ToString("D2");

            return new Tuple<string, string>($"{year}{month}{day}", $"{hour}{minute}{second}");
        }

        public static Tuple<string, string> ToYmdHmsTuple(this string ymdhms)
        {
            DateTime? dt = ymdhms.ToDateTime();
            if (dt.HasValue)
                return dt.Value.ToYmdHmsTuple();

            return null;
        }

        public static string ToYYYY(this DateTime? dt) => dt.Value.Year.ToString("D4");

        public static string ToMM(this DateTime? dt) => dt.Value.Month.ToString("D2");

        public static string ToDD(this DateTime? dt) => dt.Value.Date.ToString("D2");
    }
}
