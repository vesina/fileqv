using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Linq;

namespace csvtosql
{
    public class FileHelper
    {
        private readonly string _PATH;
        private readonly string _FILE;
        private readonly char _QUOTE;
        //private bool _IS_QUOTED;
        public readonly char _DELIM;
        private readonly string _OUTPUT_ROWS = @"_out_rows_" + DateTime.Now.ToString("yyyy-dd-M--HH-mm-ss");
        private readonly string _ERR_LOG= "_err_" + DateTime.Now.ToString("yyyy-dd-M--HH-mm-ss");

        public FileHelper()
        {
        }
        public FileHelper(string file)
        {            
            _PATH = System.IO.Directory.GetCurrentDirectory();
            _FILE = file;
        }
        public FileHelper(string path, string file)
        {
            _PATH = path;
            _FILE = file;
        }
        public FileHelper(string path, string file, char delim, string quote)
        {
            _PATH = path;
            _FILE = file;
            
            if (quote.Length > 0)
            {
                _QUOTE = quote.ToCharArray()[0];
                //_IS_QUOTED = true;
            }
            _DELIM = delim;
        }
        public bool IsQuoted()
        {
            if ((_QUOTE.ToString().Length == 0))
                return false;
            else
                return true;
        }
        public string ParseQuotedString(string s)
        {
            if (!IsQuoted())
                return s;

            char[] chAr = new char[] { _QUOTE, _QUOTE};

            if ((s == "") || (s == chAr.ToString()) || (s.Length == 0) || (s.Trim().Replace("\n", "").Replace("\t", "").Length == 0)
                || (s.Trim().Replace("\n", "").Replace("\t", "") == chAr.ToString()))
                return "";
            //if ((IsQuoted()) && (s.ToCharArray()[0] == _QUOTE) && (s.ToCharArray()[s.ToCharArray().Length - 1] == _QUOTE))
            
            //if ((s.ToCharArray()[0] == _QUOTE) && (s.ToCharArray()[s.ToCharArray().Length - 1] == _QUOTE))
            //{
            //    s = s.Substring(1, (s.Length - 3)); // removes the quotes from the string
            //}
            return s = s.Trim(_QUOTE);//Replace("\"", "");
        }
        public string ParseQuotedString(string s, bool is_num)
        {
            if (!IsQuoted())
                return s;
            if (is_num)
                return s = s.Replace("\"", "");
            char[] chAr = new char[] { _QUOTE, _QUOTE };

            //if ((s == "") || (s == chAr.ToString()) || (s.Length == 0) || (s.Trim().Replace("\n", "").Replace("\t", "").Length == 0)
            //    || (s.Trim().Replace("\n", "").Replace("\t", "") == chAr.ToString()))
            //    return "";
            //if ((IsQuoted()) && (s.ToCharArray()[0] == _QUOTE) && (s.ToCharArray()[s.ToCharArray().Length - 1] == _QUOTE))

            //if ((s.ToCharArray()[0] == _QUOTE) && (s.ToCharArray()[s.ToCharArray().Length - 1] == _QUOTE))
            //{
            //    s = s.Substring(1, (s.Length - 3)); // removes the quotes from the string
            //}
            return s = s.Trim(_QUOTE);// Replace("\"", "");
        }
        public List<string> BuildSrcTable(ref DataTable src_dt, ref DataTable tdt_dt, string line)
        {
            /// Error list to return
            List<string> err = new List<string>();
            /// List of fields in the file from the first line
            List<string> fl_lst = line.Split(_DELIM).AsEnumerable().Select(s => s.Trim().Trim('"')).ToList();
            /// List of columns in the source template table 
            // List<string> src_lst = src_dt.Columns.Cast<DataColumn>().Select(col => col.ColumnName).ToList();
            /// List of fields in target table (to compoare the the file list and build bcp table)
            List<string> tgt_lst = tdt_dt.Rows.Cast<DataRow>().Select(row => row[1].ToString()).ToList();
            /// Common fields existing in both file and target
            List<string> intersect = fl_lst.Intersect(tgt_lst).ToList(); 
            /// Fields in file , missing from target table - added as ERRORS to error list
            List<string> except_fil = fl_lst.Except(tgt_lst).ToList();  // exist in file but not in target table || Add to error_list
            err.Add(string.Format("{0}: Field{1} [{2}] missing from target table.", "W"
                , (except_fil.Count >1 ? "s" : ""), string.Join(",", except_fil)));
            /// Fields in target, not in file: 
            /// if target.Column.IS_NULLABLE -> if param.NoValue.ACCEPTED -> OK
            List<string> except_tab = tgt_lst.Except(fl_lst).ToList();  // exist in target tablebut not in file //
            err.Add(string.Format("WARNING\nExtra fields in source file [{0}].\nMissing fields in source file [{1}].", string.Join(",", fl_lst.Except(tgt_lst)), string.Join(",", tgt_lst.Except(fl_lst))));
            try

            {
                int colix = 0;
                foreach (string instr in fl_lst)//intersect)
                {
                    //string instr = s;//.TrimStart().TrimEnd();
                    bool colExists = false;
                    //instr = ParseQuotedString(instr);
                    foreach (DataRow row in tdt_dt.Rows)
                    {
                        //Console.WriteLine(row[1].ToString().TrimStart().TrimEnd() + " " + s.Trim());
                        if (row[1].ToString().TrimStart().TrimEnd() == instr)
                        {
                            //colExists = true;
                            DataColumn dc = new DataColumn(instr);

                            dc = SetDataColumn(instr, row);

                            //src_dt.Columns.Add(dc);
                            src_dt.Columns[instr].SetOrdinal(colix);

                            break;
                        }
                    }
                    colix += 1;
                    // check if file fields correspond to fields in the target table and error out if not
                    if (!colExists)
                    {
                        err.Add(string.Format("File field [{0}] does not exist in the destination table.", instr));
                        err.Add(string.Format("Error: File field [{0}] does not exist in the destination table.", instr));
                        //throw new CustomExeptions(string.Format("File field [{0}] does not exist in the destination table.", instr));
                    }

                }
            }
            catch(Exception e)
            {
                while (e.InnerException != null) e = e.InnerException;
                err.Add(e.Message.ToString());
            }
            return err;
        }
        private DataColumn SetDataColumn(string colname, DataRow dr)
        {
            DataColumn dc = new DataColumn(colname); 
            switch (dr[2].ToString().ToLower())
            {
                case "int":
                case "smallint":
                case "tinyint":
                    dc.DataType = typeof(int);
                    break;
                case "bigint":
                    dc.DataType = typeof(long);
                    break;
                case "decimal":
                case "numeric":
                    dc.DataType = typeof(decimal);
                    break;
                case "datetime":
                    dc.DataType = typeof(DateTime);
                    break;
                //case "date":
                //    dc.DataType = typeof(DateTime);
                //break;
                case "nvarchar":
                case "varchar":
                case "char":
                case "nchar":
                    dc.DataType = typeof(string);
                    dc.MaxLength = (int)dr[6];
                    break;
                default:
                    dc.DataType = typeof(string);
                    //dc.MaxLength = (int)dr[6];
                    break;
            }
            dc.AllowDBNull = (int)dr[5] == 0 ? true : false;
            return dc;
        }
        public void PrintFields (DataTable srcdt)
        {
            StringBuilder sb = new StringBuilder(string.Format("Table has {0} fields.\n", srcdt.Columns.Count.ToString()));
            foreach (DataColumn col in srcdt.Columns)
            {
                sb.AppendLine(col.ColumnName + "\t: " + col.DataType.Name + "\t= " + col.DataType.ToString());
            }
            Console.WriteLine(sb.ToString());
        }

        string GetFullName()
        {
            return _PATH + @"\" + _FILE;
        }
        public string GetFullName(string filename)
        {
            return _PATH + @"\" + filename;
        }
        public ArrayList ListFileCols()
        {
            StreamReader sr = new StreamReader(GetFullName());
            ArrayList arrayList = new ArrayList();
            // read header as column names
            foreach (string s in sr.ReadLine().Split(','))
            {
                arrayList.Add(s.TrimStart().TrimEnd());
            }

            sr.Close();
            return arrayList;
        }
        public ArrayList ListFileCols(string file, bool debug)
        {
            StreamReader sr = new StreamReader(file);
            ArrayList arrayList = new ArrayList();
            // read header as column names
            foreach (string s in sr.ReadLine().Split(','))
            {
                arrayList.Add(s.TrimStart().TrimEnd());
            }

            sr.Close();
            return arrayList;
        }
        public ArrayList ListFileCols(string line, object parser)
        {
            ArrayList arrayList = new ArrayList();
            // read header as column names
            foreach (string s in line.Split(','))
            {
                arrayList.Add(s.TrimStart().TrimEnd());
            }
            return arrayList;
        }

        public void OutputToFile(string outputfile, string line)
        {

            DateTime DateTime = DateTime.Now;
            using (StreamWriter sw = File.CreateText(outputfile))
            {
                sw.WriteLine(line);
            }
        }
        public void OutputToFile(string outputfile, DataTable table)
        {

            DateTime DateTime = DateTime.Now;
            using (StreamWriter sw = File.CreateText(outputfile))
            {

                foreach (DataRow row in table.Rows)
                {
                    foreach(DataColumn dc in table.Columns)
                    {
                        sw.WriteLine(row[dc.ColumnName].ToString());
                    }
                    
                }
            }
        }
    }
}
