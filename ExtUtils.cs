using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Data;


namespace csvtosql
{
    public static partial class ExtUtils
    {

        public static string StringTrim(this string s)
        {
            char[] charsToTrim = { '*', ' ', '\'', '"','[',']',',', '\'' };
            return s.Trim(charsToTrim);
        }
        public static void ConsoleTester()
        {
            Console.WriteLine("Input Regex string:");
            string restr = Console.ReadLine();
            string val = "";
            if (val == "q") return;
            do
            {
                Console.WriteLine("Input value:");

                val = Console.ReadLine();  
                if (val == "xx")
                {
                    Console.WriteLine("Input Regex string:");
                    restr = Console.ReadLine();
                    Console.WriteLine("Input value:");
                    val = Console.ReadLine();
                }
                Regex re = new Regex(restr, RegexOptions.IgnoreCase);
                bool b = re.IsMatch(val);
                Console.WriteLine(val + " -> result: " + b.ToString());
            } while ((val != "q"));
        }
        public static double? ToNullableDouble(this string s)
        {
            if (double.TryParse(s, out double n)) return n;
            return null;
        }
        public static DateTime? ToNullableDateTime(this string s)
        {
            if (DateTime.TryParse(s, out DateTime temp))
                return temp;
            else
                return null;
        }
        public static DateTime ToDateTime(this string s)
        {
            DateTime temp;           
            String[] formats = { "yyyyMMdd", "MMddyyyy", "ddMMyyyy", "yyMMdd", "MMddyy", "ddMMyy", "ddMMyyyy" };
            try
            {
                temp = DateTime.Parse(s, System.Globalization.CultureInfo.InvariantCulture); 
            }
            catch
            { 
                try
                {
                    temp = DateTime.ParseExact(s, formats, null, System.Globalization.DateTimeStyles.None);
                }
                catch
                {
                    return DateTime.MinValue;
                }
            }
            return temp;
        }
        public static bool IsDateTime(this string s)
        {
            DateTime temp;
            String[] formats = { "yyyyMMdd", "MMddyyyy", "ddMMyyyy", "yyMMdd", "MMddyy", "ddMMyy", "ddMMyyyy" };
            try
            {
                temp = DateTime.Parse(s, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch
            {
                try
                {
                    temp = DateTime.ParseExact(s, formats, null, System.Globalization.DateTimeStyles.None);
                }
                catch
                {
                    return false;
                }
            }
            return true;
        }
        public static string Quotename(this string s, string quote)
        {
            if ((quote == "[") || (quote == "]"))
                return string.Format("[{0}]", s);
            else
            if ((quote == "{") || (quote == "}"))
                return string.Format("{0}{1}{2}", "{", s, "}");
            else
            if ((quote == "(") || (quote == ")"))
                return string.Format("{0}{1}{2}", "(", s, ")");
            return string.Format("{0}{1}{2}", quote, s, quote);
        }
        public static bool IsDate(this string input)
        {
            return input.IsDateTime();
            //if ((new Regex(@"\D")).IsMatch(input) || (input.Length < 8))
            //    return false;
            //bool b = false;
            //if ((input.Length == 6))
            //    b = int.Parse(input) > 0;
            //else
            //{
            //    DateTime temp;
            //    if (input.Length == 8)
            //    {

            //        b = (DateTime.TryParse((input.Insert(6, "-").Insert(4, "-")), out temp));//(input.Insert(6, "-").Insert(4, "-")).IsDateTime();
            //    }
            //    else
            //        b = (DateTime.TryParse(input, out temp));
            //}
            //return b;
        }
        public static string RemoveQuotes(this string s, char quote)
        {
            char[] chAr = new char[] { quote, quote };

            if ((s == "") || (s == chAr.ToString()) || (s.Length == 0) || (s.Trim().Replace("\n", "").Replace("\t", "").Length == 0)
                || (s.Trim().Replace("\n", "").Replace("\t", "") == chAr.ToString()))
                return "";

            return s.Trim(quote);
        }
        public static bool ToBool(this string s)
        {
            if ((s == "0") || (s.ToLower() == "false"))
                return false;
            else
                return true;
        }
        public static DateTime ToDate(this string input)
        {
            //if (input.IsDateTime())
                return input.ToDateTime();
            //if (!(new Regex(@"\D")).IsMatch(input) || (input.Length < 8))
            //    return DateTime.MinValue;
            //DateTime temp;
            //if (DateTime.TryParse(input, out temp))
            //    return temp;
            //else
            //if (DateTime.TryParse((input.Insert(input.Length - 2, "-").Insert(input.Length - 2, "-")), out temp)) 
            //    return temp;
            //else
            //    return DateTime.MinValue;
        }
        public static string ToDateString(this string input)
        {
            //if (!(new Regex(@"\D")).IsMatch(input) || (input.Length < 8))
            //    return "";

            //DateTime temp;
            if (input.IsDateTime())
                return input.ToDateTime().ToString();
            //if (DateTime.TryParse(input, out temp))
            //    return input;
            //else
            //if (DateTime.TryParse((input.Insert(input.Length - 2, "-").Insert(input.Length - 2, "-")), out temp))
            //    return input.Insert(input.Length - 2, "-").Insert(input.Length - 2, "-");
            else
                return ""; 
        }
        public static bool IsBit(this string input)
        {
            bool b;
            switch(input.ToLower())
            {
                case "true":
                case "false":
                case "0":
                case "1":
                case "00":
                    b = true;
                    break;
                default:
                    b = false;
                    break;
            }
            return b;
        }
        public static bool ToBit(this string input)
        {
            bool b;
            switch (input.ToLower())
            {
                case "true":
                case "1":
                    b = true;
                    break;
                case "false":
                case "0":
                case "00":
                    b = true;
                    break;
                default:
                    b = false;
                    break;
            }
            return b;
        }
        public static bool IsDecimal(this string input)
        {
            try
            {
                decimal.TryParse(input, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out decimal num);
                Console.WriteLine(num);
            }
            catch
            {
                return false;
            }
            return true; // (new Regex(@"^(^\d*.\d*$)|(^\d*$)$")).IsMatch(input);
        }
        public static decimal ToDecimal(this string input)
        {             
            return Decimal.Parse(input, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture);
        }
        public static decimal ToInteger(this string input)
        {
            return long.Parse(input, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture);
        }
        public static decimal? ToNullableDecimal(this string s)
        {
            if (decimal.TryParse(s, out decimal n)) return n;
            return null;
        }
        public static bool IsEmptyString(this string input)
        {
            return (!(new Regex(@".+")).IsMatch(input));
        }
        public static bool IsNumericType(this DataColumn col)
        {
            if (col == null)
                return false;
            // Make this const
            var numericTypes = new[] { typeof(Byte), typeof(Decimal), typeof(Double),
        typeof(Int16), typeof(Int32), typeof(Int64), typeof(SByte),
        typeof(Single), typeof(UInt16), typeof(UInt32), typeof(UInt64)};
            return numericTypes.Contains(col.DataType);
        }
        public static string[] SplitTextToFields(this string input, char ch)
        {
            string rx = string.Format("[{0}](?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))", ch.ToString());
            string[] arr = Regex.Split(input, rx);
            for (int i = 0; i < arr.Length; i++)
            {
                if ((arr[i].StartsWith("\"")) && (arr[i].EndsWith("\"")))
                    arr[i] = arr[i].Trim('\"');
            }
            return arr;
        }
        //public static string[] SplitTextToFields(this string input, char ch, bool is_quoted)
        //{
        //    string rx = string.Format("[{0}](?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))", ch.ToString());
        //    string[] arr = Regex.Split(input, rx);
        //    for (int i = 0; i < arr.Length; i++)
        //    {
        //        if ((arr[i].StartsWith("\"")) && (arr[i].EndsWith("\"")))
        //            arr[i] = arr[i].Trim('\"');
        //    }
        //    return arr;
        //}
        public static Func<bool, string, bool, bool> join_bool = (oldvalue, joinstr, newvalue) =>
        {
            bool res;
            switch (joinstr.ToLower())
            {
                case "&&":
                case "&":
                case "and":
                    res = oldvalue && newvalue;
                    break;
                case "||":
                case "|":
                case "or":
                    res = oldvalue || newvalue;
                    break;
                default:
                    res = false;
                    break;
            }
            return res;
        };

        public static Func<string, string, string, bool> compare_two_num = (lvalue, comp, rvalue) => {
            bool is_valid;
            if ((rvalue.StringTrim() == string.Empty) || (rvalue.StringTrim() == null) || (rvalue.StringTrim().ToUpper() == "NULL") || (rvalue.StringTrim() == ""))
            {
                /// This is case is when comparing to NULL or blank string
                /// version 2.3.0
                switch (comp)
                {
                    case "!=":
                    case "<>":
                        is_valid = lvalue.ToString() != string.Empty;
                        break;
                    case "=":
                    case "==":
                        is_valid = lvalue.ToString() == string.Empty;
                        break;
                    default:
                        is_valid = false;
                        break;
                }
            }
            else
            {
                switch (comp)
                {
                    case ">":
                        is_valid = decimal.Parse(lvalue.ToString()) > decimal.Parse(rvalue.ToString());
                        break;
                    case ">=":
                        is_valid = decimal.Parse(lvalue.ToString()) >= decimal.Parse(rvalue.ToString());
                        break;
                    case "<":
                        is_valid = decimal.Parse(lvalue.ToString()) < decimal.Parse(rvalue.ToString());
                        break;
                    case "<=":
                        is_valid = decimal.Parse(lvalue.ToString()) <= decimal.Parse(rvalue.ToString());
                        break;
                    case "=":
                    case "==":
                        is_valid = decimal.Parse(lvalue.ToString()) == decimal.Parse(rvalue.ToString());
                        break;
                    case "!=":
                    case "<>":
                        is_valid = decimal.Parse(lvalue.ToString()) != decimal.Parse(rvalue.ToString());
                        break;
                    default:
                        is_valid = false;
                        break;

                }
            }

            return is_valid;
        };
        public static Func<string, string, string, bool> compare_two_str = (lvalue, comp, rvalue) => {
            bool is_valid;
            switch (comp)
            {
                case "!=":
                case "<>":
                    is_valid = lvalue != rvalue;
                    break;
                case "=":
                case "==":
                    is_valid = lvalue == rvalue;
                    break;
                default:
                    is_valid = false;
                    break;

            }
            return is_valid;
        };
        public static Func<string, string, string, bool> compare_two_dates = (lvalue, comp, rvalue) => {
            bool is_valid;
            if ((rvalue.StringTrim() == "") || (rvalue.StringTrim().ToUpper() == "NULL") || (rvalue == null)) 
            /// This is case is when comparing to NULL or blank string
            /// version 2.3.0
            {
                switch (comp)
                {
                    case "!=":
                    case "<>":
                        is_valid = lvalue.StringTrim() != string.Empty;
                        break;
                    case "=":
                    case "==":
                        is_valid = lvalue.StringTrim() == string.Empty;
                        break;
                    default:
                        is_valid = false;
                        break;

                }
            }
            else
            if (((lvalue == null) || (rvalue == null)) || ((!lvalue.IsDateTime()) && (!rvalue.IsDateTime()) && !(lvalue.IsDate()) && (!rvalue.IsDate())))
                return false;
            else
                switch (comp)
                {
                    case "!=":
                    case "<>":
                        is_valid = lvalue.ToDateTime() != rvalue.ToDateTime();
                        break;
                    case "=":
                    case "==":
                        is_valid = lvalue.ToDateTime() == rvalue.ToDateTime();
                        break;
                    case ">":
                        is_valid = lvalue.ToDateTime() > rvalue.ToDateTime();
                        break;
                    case ">=":
                        is_valid = lvalue.ToDateTime() >= rvalue.ToDateTime();
                        break;
                    case "<":
                        is_valid = lvalue.ToDateTime() < rvalue.ToDateTime();
                        break;
                    case "<=":
                        is_valid = lvalue.ToDateTime() <= rvalue.ToDateTime();
                        break;
                    default:
                        is_valid = false;
                        break;

                }
            return is_valid;
        };

    }

}
