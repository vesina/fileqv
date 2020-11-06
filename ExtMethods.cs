using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace csvtosql
{
    public static partial class ExtUtils
    {
        public static DateTime getdate() { return DateTime.Now; }
        public static DateTime getdate3(int y, int m, int d)
        {
            DateTime dt = DateTime.Now;
            y = y < 1 || y > 9999 ? dt.Year : y;
            m = m < 1 || m > 12 ? dt.Month : m;
            d = d < 1 || d >= DateTime.DaysInMonth(y, m) ? DateTime.DaysInMonth(y, m) : d;
            return new DateTime(y, m, d);
        }
        public static DateTime getdate4(int y, int m, int d, string dtstr)
        {
            DateTime dt = dtstr.IsDateTime() ? dtstr.ToDateTime() : DateTime.Now;
            y = y < 1 || y > 9999 ? dt.Year : y;
            m = m < 1 || m > 12 ? dt.Month : m;
            d = d < 1 || d >= DateTime.DaysInMonth(y, m) ? DateTime.DaysInMonth(y, m) : d;
            return new DateTime(y, m, d);
        }
        public static DateTime getdate6(int y, int m, int d, int h, int mi, int s)
        {
            y = y < 1 || y > 9999 ? DateTime.Today.Year : y;
            m = m < 1 || m > 12 ? DateTime.Today.Month : m;
            d = d == 0 || d >= DateTime.DaysInMonth(y, m) ? DateTime.DaysInMonth(y, m) : d;
            h = h < 0 || h > 23 ? 0 : h;
            mi = mi < 0 || mi > 59 ? 0 : mi;
            s = s < 0 || s > 59 ? 0 : s;
            return new DateTime(y, m, d, h, mi, s);

        }
        public static DateTime dateadd(string datepart, int n, string dtstr)
        {
            DateTime dt = dtstr.IsDateTime() ? dtstr.ToDateTime() : DateTime.Now;
            DateTime newdt = new DateTime();
            switch (datepart.StringTrim().Trim().ToLower())
            {
                case "y":
                case "yy":
                case "yyyy":
                    newdt = dt.AddYears(n);
                    break;
                case "m":
                case "mn":
                    newdt = dt.AddMonths(n);
                    break;
                case "d":
                case "day":
                case "days":
                case "dd":
                    newdt = dt.AddDays(n);
                    break;
                case "h":
                case "hh":
                case "hr":
                    newdt = dt.AddHours(n);
                    break;
                case "mi":
                case "mm":
                case "min":
                    newdt = dt.AddMinutes(n);
                    break;
                case "s":
                case "ss":
                case "sec":
                    newdt = dt.AddSeconds(n);
                    break;
                case "ms":
                    newdt = dt.AddMilliseconds(n);
                    break;
            }
            return newdt;
        }
        public static DateTime filedate() { return Program.configR._FILE_DATE; }

    }
}
