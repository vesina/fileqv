using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;

using System.Data;
using System.Data.SqlClient;
using System.Data.Common;
using System.Collections;
using System.IO;
using System.Threading;
using System.Text.RegularExpressions;
using System.Reflection;
using System.Configuration;
using System.Net;


namespace csvtosql
{
    class Program  
    {
        /// <summary>
        /// Objects
        /// </summary>
        public static SqlUtils sqlUtils;
        static Emailer mailMan;
        public static ConfigR configR;

        public static List<FormatWarning> _WARN_LIST;
        public static List<SumWarning> _SUM_LIST;
        public static long _CNT = 0;
        //static List<Task<long>> _TASKS;
        static StringBuilder _LINE_BUILDER;

        public static int Main(string[] args)
        {            
            DateTime start = DateTime.Now;
            configR = new ConfigR(); /// Configuration class
            mailMan = new Emailer(); /// Email class
            sqlUtils = new SqlUtils(); /// Sql class for holding local Data Set with metadata and rules
            _LINE_BUILDER = new StringBuilder(); 

            /// Prepare the logging facilities
            _WARN_LIST = new List<FormatWarning>();
            _SUM_LIST = new List<SumWarning>();
            string runtime_params = string.Empty;

            ErrorCounter counter = new ErrorCounter(1, configR._HARD_STOP, start, configR._EMAIL_BATCH);

            string result = configR.ParseCmdParams(args);
            if (result != "")
            {
                Console.WriteLine(result);
                PreExitActions(counter.email_batch, counter.pos);
                Environment.Exit(ExitApp(start));
                return 0;
            }
            /// At this point the Min Configuration is set: SERVER, DATABASE, FILE, PATH  
            /// Read config file and config table for alternatives/changes/full list of parameters
            try
            {
                configR.ReadConfigFile("");
                configR.ReadConfigTable();
                //runtime_params = configR.PrintClassFields();              //Console.WriteLine(runtime_params);

                /// Build local Data Set for Metadata and Rules
                sqlUtils._SERVER = configR._SERVER;
                sqlUtils._DATABASE = configR._DATABASE;
                sqlUtils._LOG_TABLE = configR._LOG_TABLE;
                /// Re-Set the Counter to values from ConfigR
                counter = new ErrorCounter(configR._MAX_ERRORS, configR._HARD_STOP, start, configR._EMAIL_BATCH);
            }          
            catch(Exception e)
            {
                while (e.InnerException != null) e = e.InnerException;
                counter.Add(1);
                sqlUtils.WriteToSqlLog((int)-1, e.ToString(), "Configuration failure.\n" + configR.PrintClassFields(true), configR._FILE);

                configR._EXIT_CODE = ExitCode.Error;
                PreExitActions(counter.email_batch, counter.pos);
                Environment.Exit(ExitApp(start));
            }

            /// Build local Data Set 
            try
            {
                sqlUtils.GetLocalDataSet(configR._FILE);
                if ((sqlUtils._dsERRORS.Count > 0) 
                    || (sqlUtils._LocalDS.Tables.Count == 0)
                    || (sqlUtils._LocalDS.Tables[1].Rows.Count == 0)
                    || (sqlUtils._LocalDS.Tables[2].Rows.Count == 0))
                    throw new CustomException("E:Errors building metadata.");
            }
            catch (Exception e)
            {
                sqlUtils.WriteToSqlLog((int)-1, e.ToString(), "Runtime parameters:\n{0}" + configR.PrintClassFields(true), configR._FILE);
                sqlUtils._dsERRORS.ForEach(x =>
                {
                    sqlUtils.WriteToSqlLog((int)-1, x, "W: Memory dataset", configR._FILE);
                });
                counter.Add(sqlUtils._dsERRORS.Count(), (int)ExitCode.Error);
            }
            /// Update Email recipients from FileInfo
            if (sqlUtils._LocalDS.Tables["FileInfo"].Columns.Contains("email_to"))
                configR._EMAIL_TO = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["email_to"].ToString();
            if (sqlUtils._LocalDS.Tables["FileInfo"].Columns.Contains("email_cc"))
                configR._EMAIL_CC = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["email_cc"].ToString();
            if (sqlUtils._LocalDS.Tables["FileInfo"].Columns.Contains("email_bc"))
                configR._EMAIL_BC = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["email_bc"].ToString();
            if (sqlUtils._LocalDS.Tables["FileInfo"].Columns.Contains("delimiter"))
                configR._DELIM = (char)(int.Parse(sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["delimiter"].ToString()));
            mailMan.smtpserver = configR._SMTP_SERVER;
            mailMan.from_email = configR._SENDER;
            mailMan.to_email = configR._EMAIL_TO;
            mailMan.cc_email = configR._EMAIL_CC;
            mailMan.bc_email = configR._EMAIL_BC;

            /// At this point all Configuretion parameters are set
            runtime_params = PrintClassFields(configR);

            if (configR._DEBUG)
            {
                Console.WriteLine(runtime_params);
                Console.WriteLine(string.Format("Processing file {0}. Please wait ...", configR._FILE));
            }

            List<int> indexes = new List<int>();           

            List<SqlParameter> warn_parameters = sqlUtils._SQL_PARAMETERS;
            //List<string> _blank_pk_cols = sqlUtils._PK_COLS.Select(p => "").ToList();

            //configR._DELIM = (char)(int.Parse(sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["delimiter"].ToString()));

            DataTable srcdt = sqlUtils._LocalDS.Tables["Table"];
            List<string> lst_tabcols = sqlUtils._LocalDS.Tables["Table"].Columns.OfType<DataColumn>().Select(c => c.ColumnName).ToList();

            ///// This is needed if load to file or BCP to SQL table
            ///// Create lists for the asynch tasks for batch loading and error mesggages. 
            //if ((_OPTIONS == ValidationOptions.LoadToFile) && (_OPTIONS == ValidationOptions.LoadToTableBCP))
            //    _TASKS = new List<Task<long>>();    
      
            /// Create dataset for the temporary tables that will BCP to SQL 
            //_DS = new DataSet();
            
            StreamReader sr = new StreamReader(GetFullName(), System.Text.Encoding.Default, true);
            string line = "";
            line = sr.ReadLine();
            string[] ar_filecols = line.Split(configR._DELIM).AsEnumerable().Select(s => s.Trim().Trim('"')).ToArray();
            List<string> lst_filecols = line.Split(configR._DELIM).AsEnumerable().Select(s => s.Trim().Trim('"')).ToList();

            StringBuilder err_sb = new StringBuilder();

            /// Parse File HEADER for: column order, delimiter, missing/added columns
            try 
            {   
                string err = ParseFileHeaderFormating(line, lst_tabcols, configR._DELIM);//, ref counter);
                if (err.ToString().Length > 0)
                {
                    configR._EXIT_CODE = ExitCode.Error;
                    throw new CustomException(err);
                }
            }
            catch(Exception e)
            {
                while (e.InnerException != null) e = e.InnerException;

                FormatWarning fmt_warn = new FormatWarning();
                fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                fmt_warn.SetValues("HEADER", configR._FILE, e.Message.ToString(), _CNT, "Invalid header.");
                _WARN_LIST.Add(fmt_warn);

                sqlUtils.WriteToSqlLog((int)_CNT, e.Message.ToString(), line, configR._FILE);
                counter.Add(1, 2);
                if ((configR._EXIT_CODE == ExitCode.Error))
                {
                    //sr.Close();
                    PreExitActions(counter.email_batch, counter.pos);
                    Environment.Exit(ExitApp(start));  
                }
            } 
            /// Setup the Rules 
            List<DataColumn> _FIELDS = sqlUtils._LocalDS.Tables["Table"].Columns.OfType<DataColumn>().ToList();
            PropertyCollection[] rules_line = sqlUtils._LocalDS.Tables["Table"].Columns.OfType<DataColumn>().Select(c => c.ExtendedProperties).ToArray<PropertyCollection>();
            /// Create list for checking all trackable fields - if all values in the field ar NULL or BLANK - raise error - ALL NULLS should be less than All ROWS
            List<int> checkAllNulls = sqlUtils._CheckAllNulls;

            //// add blank table to the dataset: this is for Load to Sql (BCP)
            DataTable src_dt = srcdt.Clone();
            src_dt.TableName = "Table";
            //_DS.Tables.Add(src_dt);
            //int ix = _DS.Tables.Count - 1;
            long _ROWS = 0;

            //try
            //{                
            while ((line = sr.ReadLine()) != null)
            {    /// Add DataRows to table
                _ROWS++;
                
                if (sr.EndOfStream) /// Check for footer message in the line
                {
                    string[] localar = line.SplitTextToFields(configR._DELIM);
                    int ixr = localar.Where(itm => itm == "").Count();
                    string localst = localar.Where(itm => itm != "").FirstOrDefault();
                    if ((ixr == localar.Length -1) && (localst.StringTrim().Split(' ').FirstOrDefault().ToLower() == "copyright"))
                        break;
                }
                try
                {                        
                    DataRow dr = srcdt.NewRow();
                    _CNT++;
                    dr = ReadLineToRow(line, dr, _CNT, sqlUtils, ref rules_line, ref checkAllNulls, ref counter);
                    if((dr != null) && (((int)configR._VALIDATION_OPTIONS & (int)ValidationOptions.ValidateAsTable) == (int)ValidationOptions.ValidateAsTable))
                    {
                        /// Add DataRow to the table - get all ROW level errors from SQL
                        srcdt.Rows.Add(dr);
                        //_DS.Tables[ix].ImportRow(dr);
                    }
                }
                catch (Exception e)
                {
                    while ((e.InnerException != null) && (e.Message != @"Input string was not in a correct format.")) e = e.InnerException;
                        
                    string[] ar = line.SplitTextToFields(configR._DELIM);
                    List<string> pvalues = new List<string>();
                    pvalues = srcdt.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString()).ToList();

                    FormatWarning fmt_warn = new FormatWarning();
                    fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                    fmt_warn.SetValues(pvalues, "'Sql errors'", "''", ("E: " + e.ToString().Replace("'", "''")).Quotename("'"), _CNT, @"Input string was not in a correct format." );
                    _WARN_LIST.Add(fmt_warn);
                        
                    pvalues.Clear();

                    counter.Add(1, (int)ExitCode.Warning);
                    continue;
                }
            } 
 
            //sr.Close();
            //_DS.Dispose();                 

            if (checkAllNulls.Count() > 0) /// There are totally blank fields that should have some values
            {
                bool is_err = (sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["warn_all_nulls"].ToString()).ToBool();
                foreach (string s in checkAllNulls.Select(o => _FIELDS[o].ColumnName + "(" + _FIELDS[o].Caption + ")").ToList())
                {
                    List<string> pvalues = new List<string>();
                    pvalues = srcdt.PrimaryKey.Select(p => "''").ToList();
                        
                    FormatWarning fmt_warn = new FormatWarning();
                    fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                    fmt_warn.SetValues(pvalues, s.Quotename("'")
                        , "<NULL>", "'"+ (is_err ? "E" : "W")+": Field does not have any values (blank/NULL)'"
                        , _CNT
                        , "Field does not have values");
                    _WARN_LIST.Add(fmt_warn);

                    pvalues.Clear();

                    counter.Add(1, (is_err ? (int)ExitCode.Error : (int)ExitCode.Warning));
                    PreExitActions(counter.email_batch, counter.pos);
                    Environment.Exit(ExitApp(start));
                }
            }

            PreExitActions(counter.email_batch, counter.pos);
            //sr.Close();
            //sr.Dispose();
            sqlUtils = null;
            Console.WriteLine("Processed {0} rows in {1} sec.\nApplication exit with code {2} ({3})", _CNT
                , DateTime.Now.Subtract(start).TotalSeconds
                , (int)configR._EXIT_CODE, configR._EXIT_CODE.ToString());
            return (int)configR._EXIT_CODE;
        }

        public static object CallStaticMethod(string typeName, string methodName)
        { 
            try
            {               
                Type type = Type.GetType(typeName);
                MethodInfo custMethod = type.GetMethod(methodName);
                object custValue = custMethod.Invoke(type, new object[] { });

                return custValue;
            }
            catch (Exception e)
            { 
                sqlUtils.WriteToSqlLog((int)_CNT, "W:* " + e.Message.ToString(), string.Format("CallStaticMethod(\"{0}\",\"{1}\")", typeName, methodName), configR._FILE);
                return null;
            }
        }
        public static object CallStaticMethod(string typeName, string methodName, object[] pars)
        {
            try
            {
                Type type = Type.GetType(typeName);
                MethodInfo custMethod = type.GetMethod(methodName);
                ParameterInfo[] pi = custMethod.GetParameters();
                object[] pipars = new object[pi.Count()];
                if (pars.Count() > 0)
                    for (int i = 0; i < pi.Count(); i++)
                    {
                        if ((pi[i].ParameterType == typeof(int)) || (pi[i].ParameterType == typeof(long)))
                            pipars[i] = int.Parse(pars[i].ToString().Trim().StringTrim());
                        else
                        if ((pi[i].ParameterType == typeof(decimal)) || (pi[i].ParameterType == typeof(float)))
                            pipars[i] = decimal.Parse(pars[i].ToString().Trim().StringTrim());
                        else
                        if ((pi[i].ParameterType == typeof(DateTime)))
                            pipars[i] = pars[i].ToString().Trim().StringTrim().ToDateTime();
                        else
                            pipars[i] = pars[i];//Value

                    }

                object custValue;
                if (pars.Count() > 0)
                    //custValue = custMethod.Invoke(custObject, pipars);
                    custValue = custMethod.Invoke(type, pipars);
                else
                    //custValue = custMethod.Invoke(custObject, new object[] { });
                    custValue = custMethod.Invoke(type, new object[] { });


                return custValue;

            }
            catch (Exception e)
            {                
                sqlUtils.WriteToSqlLog((int)_CNT, e.Message.ToString()
                    , string.Format("CallStaticMethod(\"{0}\",\"{1}\",[{2}])"
                    , typeName, methodName, string.Join(",", pars)), "");
                return null;
            }

        }
        public static string[] GetStaticMethodParameters(string typeName, string methodName)
        {
            try
            {
                Type type = Type.GetType(typeName);
                MethodInfo custMethod = type.GetMethod(methodName);
                ParameterInfo[] pi = custMethod.GetParameters();
                return pi.OfType<ParameterInfo>().Select(p => p.ParameterType.ToString()).ToArray();
            }
            catch (Exception e)
            {
                sqlUtils.WriteToSqlLog((int)_CNT, "W:* " + e.Message.ToString(), string.Format("GetStaticMethodParameters(\"{0}\",\"{1}\")", typeName, methodName), configR._FILE);
                return new string[0] { };
            }
        }
        private static void PreExitActions(int email_batch, int pos)
        {            
            string final = ((int)configR._EXIT_CODE == 0 ? "Validation: Success" : (int)configR._EXIT_CODE == 1 ? "Validation: Warning" : "Validation: Failed");
            long errs = _WARN_LIST.Where(itm => itm.Message.StringTrim().StartsWith("E:")).Count();
            long warns = _WARN_LIST.Where(itm => itm.Message.StringTrim().StartsWith("W:")).Count();
            if ((_WARN_LIST.Count > 0) || (_SUM_LIST.Count > 0))
            {
                int rows = 0;
                if (_WARN_LIST.Count > pos)
                {
                    rows =sqlUtils.FinalWarnWriteToSqlWarnLog(_WARN_LIST, email_batch, 0);
                }
                //string final = ((int)_EXIT_CODE == 0 ? "Validation: Success" :  (int)_EXIT_CODE == 1 ? "Validation: Warning" : "Validation: Failed");
                /// 2.0.1.00 always log end result
                sqlUtils.WriteToSqlLog((long)_CNT, final
                    , string.Format("{0} warning written to the log table", pos)
                    , configR._FILE);
                //if (_DEBUG)
                //    Console.WriteLine(string.Format("{0} warning{1} written to the log table", rows, rows == 1 ? "" : "s"));
                /// Send email conditionally (if parameter enforces EmailAlways)
                bool b = false;
                UpdateWarnList();
                if  (_SUM_LIST.Count > 0)  
                    b = mailMan.SendSMTPMessage(FormatWarnEmailHeader(), FormatWarnEmailBody());//FormatWarnEmailBody("summary"));
                if (configR._DEBUG)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendFormat("{0} warning{1} written to the log table.\n", rows, rows == 1 ? "" : "s");
                    sb.AppendFormat("Recorded {0} errors & warnings for dvid {1} in table {2}.\n", _WARN_LIST.Count
                        , sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString()
                        , sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["warn_table"].ToString());
                    sb.AppendLine(b ? "Validation results were emailed." : "Failed to email validation results.");
                    Console.WriteLine(sb.ToString());
                } 
            }
        }
        private static int ExitApp(DateTime start)
        {
            //PreExitActions(email_batch);
            if ((configR._EXIT_CODE == ExitCode.None) || (configR._EXIT_CODE == ExitCode.Warning))
                configR._EXIT_CODE = ExitCode.None;
            if (configR._DEBUG)
            {
                Console.WriteLine("Processed {0} rows in {1} sec.\nApplication exit with code {2} ({3})", _CNT
                    , DateTime.Now.Subtract(start).TotalSeconds
                    , ((int)configR._EXIT_CODE).ToString(), configR._EXIT_CODE.ToString());
            }
                
            return (int)configR._EXIT_CODE; 
        }
        static string GetFullName()
        {
            return GetFullName(configR._FILE);
        }
        static string GetFullName(string filename)
        {
            return configR._PATH + @"\" + filename;
        }
        public static object GetFieldValue(string field, string cls)
        {
            object o = new object();
            //string ns = "csvtosql";
            Type t = Type.GetType(cls);
            FieldInfo[] FieldInfos = t.GetFields();
            for (int i = 0; i < FieldInfos.Length; i++)
            {
                if (FieldInfos[i].Name.ToLower() == field.ToLower())
                    o = FieldInfos[i].GetValue(FieldInfos[i].ToString()).ToString();
            }
            return o;
        }
        static string PrintClassFields(ConfigR obj)
        {
            Console.WriteLine(obj.GetType());
            StringBuilder sb = new StringBuilder();
            Type t = Type.GetType(obj.GetType().ToString());// "csvtosql.ConfigR");//"cons_linq.Program"
            FieldInfo[] FieldInfos = t.GetFields();
            sb.AppendLine(string.Format("Runtime parameters {0}", FieldInfos.Count().ToString()));
            for (int i = 0; i < FieldInfos.Length; i++)
            {
                try
                {
                    if ((FieldInfos[i] != null) && (FieldInfos[i].Name.ToString().StartsWith("_")))
                        sb.AppendLine(string.Format("{0}={1}", FieldInfos[i].Name.ToString(), FieldInfos[i].GetValue(obj).ToString()));
                }
                catch (Exception)
                {
                    if ((FieldInfos[i] != null) && (FieldInfos[i].Name.ToString().StartsWith("_")))
                        sb.AppendLine(string.Format("{0}={1}", FieldInfos[i].Name.ToString(), "NULL"));
                }
            }
            return sb.ToString();
        }
        public static Dictionary<string, string> ReadConfigFile(string configfile)
        {
            if (configfile == "")
            {
                string path = System.AppDomain.CurrentDomain.BaseDirectory;
                DirectoryInfo di = new DirectoryInfo(path);
                FileInfo fi = di.GetFiles("*.conf.ini").FirstOrDefault();
                if (fi != null)
                    configfile = fi.FullName;
            }
            Dictionary<string, string> config = new Dictionary<string, string>();
            using (StreamReader sr = new StreamReader(configfile))
            {
                string s;
                while ((s = sr.ReadLine()) != null)
                {
                    if (s.ToCharArray().Contains('#'))
                        s = s.Split('#')[0];
                    if (s.Length > 0)
                        config[s.Split('=').ToArray()[0].ToLower()] = s.Split('=').ToArray()[1];
                }
            }
            return config;
        }

        public static string ParseFileHeaderFormating(string file_header, IEnumerable<string> lst_tabcols, char delimiter)//, ref ErrorCounter counter)
        {
            StringBuilder err_sb = new StringBuilder();
            string header = string.Join(delimiter.ToString(), lst_tabcols);           
            /// Check is to lines match
            if (file_header.ToLower() != header.ToLower())
            {
                string delim_rx = @".*[" + delimiter.ToString() + @"].*";
                /// Check if delimiter is correct
                if (!(new Regex(delim_rx).IsMatch(file_header)))
                {
                    err_sb.AppendLine(string.Format("E: HEADER - delimiter is NOT \"{0}\".", delimiter.ToString()));
                    //_ERR_LIST.Add(err_sb.ToString());
                }
                /// Check if columns are in order
                string[] lst_filecols = file_header.Split(delimiter).AsEnumerable().Select(s => s.Trim('"')).ToArray();
                string[] lst_filecols_rx = file_header.SplitTextToFields(configR._DELIM);
                for(int i = 0; i < lst_filecols.Count(); i++)
                {
                    if (lst_tabcols.ToArray()[i].Trim() != lst_filecols[i].Trim())
                        err_sb.AppendLine(string.Format("E: HEADER - Column out of order: {0}\t{1}", i , lst_tabcols.ToArray()[i].Trim()));
                }
                var infile = lst_filecols.Select(s => s.ToLower()).Except(lst_tabcols.Select(s => s.ToLower()));
                var intab = lst_tabcols.Select(s => s.ToLower()).Except(lst_filecols.Select(s => s.ToLower()));
                if ((infile.Count()) > 0 || (intab.Count() > 0))
                {
                    infile = lst_filecols_rx.Select(s => s.ToLower()).Except(lst_tabcols.Select(s => s.ToLower()));
                    intab = lst_tabcols.Select(s => s.ToLower()).Except(lst_filecols_rx.Select(s => s.ToLower()));

                    if ((infile.Count()) > 0 || (intab.Count() > 0))
                    { 
                        err_sb.AppendLine("E: HEADER - Column mismatch:");
                        if (infile.Count() > 0)
                            err_sb.AppendLine(string.Format("New fields: {0}", string.Join(",", infile)));
                        if (intab.Count() > 0)
                            err_sb.AppendLine(string.Format("Missing fields: {0}", string.Join(",", intab)));
                    }                       
                }
            }
            return err_sb.ToString();
        }
        public static DataRow ReadLineToRow(string line, DataRow dr, long line_num, SqlUtils sqlutils, ref PropertyCollection[] rules, ref List<int> check_all_nulls, ref ErrorCounter counter)
        {
            string err_sb = "";
            string[] ar = line.SplitTextToFields(configR._DELIM);  //Split(_DELIM).ToArray();
            DataColumn[] ar_cols = dr.Table.Columns.OfType<DataColumn>().Select(c => c).ToArray();
            string[] ar_colNames = ar_cols.Select(c => c.ColumnName).ToArray();
            List<string> pvalues = new List<string>();
            pvalues.Clear();            
            if (ar.Length < ar_colNames.Length) /// add fix for LF
            {
                _LINE_BUILDER.Append(line);
                _CNT -= 1;
                ///if (_LINE_BUILDER.ToString().SplitTextToFields(_DELIM).Length < ar_colNames.Length)                   
                line = _LINE_BUILDER.ToString();
                if (line.SplitTextToFields(configR._DELIM).Length == ar_colNames.Length)
                {  
                    
                    ar = line.SplitTextToFields(configR._DELIM);
                    ReadLineToRow(line, dr, line_num, sqlutils, ref rules, ref check_all_nulls, ref counter);
                    _LINE_BUILDER.Clear();
                }
            }
            else
            //wdr["column_name"] = 
            if (ar.Length > ar_colNames.Length)
            {
                pvalues.Clear();
                if (ar.Length >= dr.Table.PrimaryKey.Select(p => p.Ordinal).Max())
                    pvalues = dr.Table.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString().Quotename("'")).ToList();          

                FormatWarning fmt_warn = new FormatWarning();
                fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                fmt_warn.SetValues(pvalues, string.Empty
                    , string.Empty
                    , string.Format("W: Line {2} has {0} fields out of {1}.", ar.Count().ToString(), ar_colNames.Count().ToString(), line_num.ToString())
                    , _CNT, "Incorrect number of fields in line.");
                _WARN_LIST.Add(fmt_warn);                
                
                pvalues.AddRange(new List<string>() { "''","''"
                    , string.Format("'W: Line {0} has {1} fields out of {2}.'", line_num, ar.Count().ToString(), ar_colNames.Count().ToString())
                    , line_num.ToString()});
                //_ERR_LIST.Add(string.Join(",", pvalues)); 
                
                pvalues.Clear();

                counter.Add(1, (int)ExitCode.Error); 
            }
            else
            {
                pvalues = dr.Table.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString().Quotename("'")).ToList();
                //List<string> pk_values = dr.Table.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString()).ToList();
                for (int i = 0; i < ar.Length; i++)
                {
                    err_sb = "";
                    string field = ar_cols[i].ColumnName;
                    string s_value = ar.GetValue(i).ToString();
                    /// Parse the value to the correct data type
                    if (ExtUtils.IsNumericType(ar_cols[i]))//if (IsNumeric(dr.Table.Columns[i]))
                        try
                        {
                            if (s_value.Trim('"').Contains('E'))
                            //Console.WriteLine(s_value.Trim('"'));
                            {/// ver 2.1.0.00
                                if ((ar_cols[i]).DataType == typeof(Decimal))
                                    s_value = ExtUtils.ToDecimal(s_value.Trim('"')).ToString();                     
                                else
                                if ((ar_cols[i]).DataType == typeof(long))
                                    s_value = ExtUtils.ToInteger(s_value.Trim('"')).ToString();
                                //else
                                    //s_value = s_value.Trim('"');

                            }/// ver 2.1.0.00

                        }
                        catch (Exception)
                        {
                            //throw new CustomException(
                            string e_msg = string.Format("Value {0} is not {1}.", s_value, ar_cols[i]);

                            //pvalues.Clear();
                            //pvalues = dr.Table.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString().Quotename("'")).ToList();

                            FormatWarning fmt_warn = new FormatWarning();
                            fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                            fmt_warn.SetValues(pvalues, field, s_value, e_msg, line_num, e_msg);
                            _WARN_LIST.Add(fmt_warn);

                            pvalues.Clear();

                            counter.Add(1, (int)ExitCode.Warning);
                            continue;
                            
                        }

                    try
                    {
                        if (check_all_nulls.Contains(i)) /// If one value is NOT NULL, 
                        {
                            if ((!s_value.IsEmptyString()))
                                check_all_nulls.Remove(i);
                        }
                        bool b = true;
                        /// PROCESS CONSTRAINTS (BUSINESS Rules)
                        if (rules[i].Keys.Count > 0)
                        {
                            foreach (string k in rules[i].Keys)
                            {
                                if (k == "ch")
                                {
                                    foreach (ConstraintEntity rl in (List<ConstraintEntity>)rules[i]["ch"])
                                    {
                                        StringBuilder rl_sb = new StringBuilder();
                                        //rl_sb.AppendLine(rl.Print());
                                        switch (rl.ConstraintType)
                                        {
                                            case ConstraintType.Regex:
                                                b = new Regex(rl.Expression).IsMatch(s_value);
                                                break;
                                            case ConstraintType.Check:   /// empty fields cannot compare, and skip if flag _SKIP_CHECK_IF_NULL = true
                                                if ((s_value != string.Empty) || ((!configR._SKIP_CHECK_IF_NULL) && (s_value == string.Empty)))
                                                    b = rl.IsConstraintValid(ar, ref rl_sb); 
                                                break;
                                            default:  
                                                b = rl.IsConstraintValid(ar, ref rl_sb);
                                                break;
                                        }
                                        if ((!b))
                                        {
                                            
                                            err_sb = string.Format("{0}: {1} {2}", rl.ErrorLevel.ToString().Substring(0, 1), rl.Description.ToString(), rl_sb.ToString());//, rl.Expression);
                                            pvalues.Clear(); 
                                            pvalues = dr.Table.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString().Quotename("'")).ToList();

                                            FormatWarning fmt_warn = new FormatWarning();
                                            fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                                            fmt_warn.SetValues(pvalues, field.Quotename("'"), s_value.Quotename("'")
                                                , err_sb.Replace("'", "''").Quotename("'")
                                                , line_num
                                                , rl.Description);
                                            _WARN_LIST.Add(fmt_warn);

                                            pvalues.AddRange(new List<string>() { field.Quotename("'"), s_value.Quotename("'"), err_sb.Replace("'", "''").Quotename("'"), line_num.ToString() });
                                            //_ERR_LIST.Add(string.Join(",", pvalues)); 
                                            
                                            pvalues.Clear();

                                            counter.Add(1, (int)rl.ErrorLevel == 2 ? 2 : 0);
                                        }
                                    }
                                }
                                else
                                if (k == "fk")
                                {
                                    List<string> lkups = ((List<string>)rules[i]["fk"]).Select(x => x.ToLower()).ToList();
                                    {
                                        /// Added check for values with trailing spaces
                                        b = lkups.Contains(s_value.ToLower()) || lkups.Contains(s_value.Trim().ToLower());
                                        if ((!b) && ((s_value != string.Empty) || ((s_value == string.Empty) && ((!configR._SKIP_CHECK_IF_NULL) || (ar_cols[i].AllowDBNull)))))
                                        {
                                            err_sb = string.Format("W: Lookup table {0}."
                                                , sqlutils._LocalDS.Tables["Relations"].Rows.OfType<DataRow>()
                                                .Where(r => int.Parse(r["ordinal"].ToString()) == i + 1)
                                                .Select(r => r["ref_table_name"].ToString() + "(" + r["ref_colname"].ToString() + ")").FirstOrDefault());

                                            pvalues.Clear(); 
                                            pvalues = dr.Table.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString().Quotename("'")).ToList();

                                            FormatWarning fmt_warn = new FormatWarning();
                                            fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                                            fmt_warn.SetValues(pvalues, field.Quotename("'"), s_value.Quotename("'")
                                                , err_sb.Replace("'", "''").Quotename("'")
                                                , line_num
                                                , err_sb.Substring(2));
                                            _WARN_LIST.Add(fmt_warn);

                                            pvalues.AddRange(new List<string>() { field.Quotename("'"), s_value.Quotename("'"), err_sb.Replace("'", "''").Quotename("'"), line_num.ToString() });
                                            //_ERR_LIST.Add(string.Join(",", pvalues));

                                            pvalues.Clear();

                                            counter.Add(1);
                                        }
                                    }
                                }
                            }
                        }
                        /// Add DataRow to the DataTable -> get all sql errors based on datatype, length, constraints, etc.
                        if ((s_value.Length == 0) || (s_value.ToUpper() == "NULL") || (s_value is null) || (s_value.Trim('"')).IsEmptyString())
                        {
                            dr[i] = DBNull.Value;
                        }
                        else
                        {
                            if (ExtUtils.IsNumericType(ar_cols[i]))//if (IsNumeric(dr.Table.Columns[i]))
                            {
                                //if (s_value.Trim('"').Contains('E'))
                                //    Console.WriteLine(s_value.Trim('"'));

                                if ((ar_cols[i]).DataType == typeof(Decimal))
                                    dr[i] = ExtUtils.ToDecimal(s_value.Trim('"'));
                                else
                                if ((ar_cols[i]).DataType == typeof(long))
                                    dr[i] = ExtUtils.ToInteger(s_value.Trim('"'));
                                else 
                                    dr[i] = s_value.Trim('"');
                            }
                            else
                            if ((ar_cols[i]).DataType == typeof(bool))//if (IsNumeric(dr.Table.Columns[i]))
                            {
                                if (s_value.IsBit())
                                    dr[i] = s_value.ToBit();
                                else
                                    dr[i] = s_value.Trim('"');
                            }
                            else
                            if (ar_cols[i].DataType == typeof(DateTime))
                            {
                                if (s_value.IsDateTime())
                                    dr[i] = s_value.ToDateTime();
                                //else
                                //if (s_value.IsDate())
                                //{
                                //    dr[i] = s_value.ToDate();

                                //}
                                else
                                    dr[i] = s_value.Trim('"').ToDateString();
                            }
                            else
                                dr[i] = (!(configR._QUOTED)) ? s_value : s_value.RemoveQuotes(configR._QUOTE);
                        }
                    }
                    catch (Exception e)
                    {
                        err_sb = "E: " + e.Message.ToString();
                        pvalues.Clear(); 
                        pvalues = dr.Table.PrimaryKey.Select(p => ar.GetValue(p.Ordinal).ToString().Quotename("'")).ToList();
 
                        FormatWarning fmt_warn = new FormatWarning();
                        fmt_warn.SetPkFields(sqlUtils._PK_COLS);
                        fmt_warn.SetValues(pvalues, field.Quotename("'"), s_value.Quotename("'")
                            , err_sb.Replace("'", "''").Quotename("'")
                            , line_num
                            , err_sb.Replace("'", "''").Quotename("'"));
                        _WARN_LIST.Add(fmt_warn);

                        pvalues.AddRange(new List<string>() { field.Quotename("'"), s_value.Quotename("'"), err_sb.Replace("'", "''").Quotename("'"), line_num.ToString() });
                        //_ERR_LIST.Add(string.Join(",", pvalues));

                        pvalues.Clear();

                        counter.Add(1);
                        configR._EXIT_CODE = ExitCode.Error;
                        continue;
                    }
                }
            }
            return dr;
        }         
        public static void OutputToFile(string outputfile, string line)
        {

            //DateTime DateTime = DateTime.Now;
            using (StreamWriter sw = File.CreateText(outputfile))
            {
                sw.WriteLine(line);
            }
        }
        public static void OutputToFile(string outputfile, DataTable table)
        {

            //DateTime DateTime = DateTime.Now;
            using (StreamWriter sw = File.CreateText(outputfile))
            {

                foreach (DataRow row in table.Rows)
                {
                    sw.WriteLine(string.Join(",", row.ItemArray));
                    //foreach (DataColumn dc in table.Columns)
                    //{
                    //    sw.WriteLine(row[dc.ColumnName].ToString());
                    //}
                }
            }
        }
        public static void OutputToFile(string outputfile, DataRow row)
        {

            //DateTime DateTime = DateTime.Now;
            using (StreamWriter sw = File.CreateText(outputfile))
            { 
                    sw.WriteLine(string.Join(",", row.ItemArray));
                    //foreach (DataColumn dc in table.Columns)
                    //{
                    //    sw.WriteLine(row[dc.ColumnName].ToString());
                    //}
 
            }
        }
        ////public static void OnSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        ////{
        ////    _ROWCOUNT += e.RowsCopied;
        ////}

        private static void MaxErrThresholdReached(object sender, ThresholdReachedEventArgs e)
        {
            Console.WriteLine("The threshold of {0} errors was reached at {1}.", e.Threshold, e.TimeReached);
            Environment.Exit((int)configR._EXIT_CODE);
        }

        public class ErrorCounter
        {
            private readonly int threshold;
            private int total;
            public int pos;
            private readonly int hardstop;
            public int email_batch = 1000;
            private readonly DateTime starttime;

            public  ErrorCounter(int passedThreshold, int hard_stop, DateTime start, int emailbatch)
            {
                threshold = passedThreshold;
                hardstop = hard_stop;
                pos = 0;
                starttime = start;
                email_batch = emailbatch;
            }

            public void Add(int x)
            {
                total += x;
                if ((total >= email_batch + pos))
                {
                    int rows = sqlUtils.FinalWriteToSqlWarnLog(_WARN_LIST, email_batch, 0);//pos);

                    /// Add code to summarize results
                    UpdateWarnList();
                    /// truncate _WARN_LIST and reposition to 0                                        
                    _WARN_LIST.Clear();
                    //pos = 0;
                    //if (_DEBUG)
                    //    Console.WriteLine(string.Format("{0} warning written to the log table", rows));
                    pos += rows;
                }
                if (((threshold > 0) && (total >= threshold)) || ((total >= hardstop) && (threshold == 0)))
                {
                    ThresholdReachedEventArgs args = new ThresholdReachedEventArgs
                    {
                        Threshold = threshold,
                        TimeReached = DateTime.Now
                    };
                    OnThresholdReached(args);
                }
            }
            public void Add(int x, int exitcode)
            {
                if ((int)configR._EXIT_CODE < exitcode)
                    configR._EXIT_CODE = (ExitCode)exitcode;

                total += x;
                if ((total >= email_batch + pos))
                {
                    int rows = sqlUtils.FinalWriteToSqlWarnLog(_WARN_LIST, email_batch, 0);//pos);
                    /// Add code to summarize results
                    UpdateWarnList();
                    /// truncate _WARN_LIST and reposition to 0                                        
                    _WARN_LIST.Clear();
                    //pos = 0;
                    //if (_DEBUG) 
                    //    Console.WriteLine(string.Format("{0} warning written to the log table", rows));
                    pos += rows;
                }
                if (((threshold > 0) && (total >= threshold)) || ((total >= hardstop) && (threshold == 0)))
                {
                    ThresholdReachedEventArgs args = new ThresholdReachedEventArgs
                    {
                        Threshold = threshold,
                        TimeReached = DateTime.Now
                    };
                    OnThresholdReached(args);
                }
            }

            protected virtual void OnThresholdReached(ThresholdReachedEventArgs e)
            {
                ThresholdReached?.Invoke(this, e);
                PreExitActions(email_batch, pos);
                Environment.Exit(ExitApp(starttime));
            }

            public event EventHandler<ThresholdReachedEventArgs> ThresholdReached;
        }
        public class ThresholdReachedEventArgs : EventArgs
        {
            public int Threshold { get; set; }
            public DateTime TimeReached { get; set; }
        }
        static string FormatWarnEmailHeader()
        {
            //string envsrv = Environment.MachineName;
            string localsrv =  Dns.GetHostName();
            string env = (configR._SERVER.ToLower().EndsWith("t") ? "(TEST)" : (configR._SERVER.ToLower().EndsWith("d") ? "(DEV)" : "(PROD)"));
            Regex rxe = new Regex("^['\"]?(E:).*");
            Regex rxw = new Regex("^['\"]?(W:).*");
            string s = string.Format("{0}: File {1} ({2}) validation results: Errors {3} Warnings {4}."
                ,  localsrv
                , configR._FILE.Quotename("\"")
                , sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["stg_tabname"].ToString()
                , _WARN_LIST.Where(itm => rxe.IsMatch(itm.Message)).Count()
                , _WARN_LIST.Where(itm => rxw.IsMatch(itm.Message)).Count());
            return s;
        }
        //static string FormatWarnEmailBody(string email_format)
        //{
        //    Regex rxe = new Regex("^['\"]?(E:).*");
        //    Regex rxw = new Regex("^['\"]?(W:).*");
        //    string env = (_SERVER.ToLower().EndsWith("t") ? "( TEST )" : (_SERVER.ToLower().EndsWith("d") ? "( DEV )" : "( PROD )"));
        //    string dvid = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString();
        //    string warn_table = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["warn_table"].ToString();
        //    string rpt_object = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["rpt_object"].ToString();

        //    int errs = _WARN_LIST.Where(itm => rxe.IsMatch(itm.Message)).Count();
        //    int warns = _WARN_LIST.Where(itm => rxw.IsMatch(itm.Message)).Count();
        //    string hdr = string.Format(" \tFile: <b>{0}</b>", _FILE);
        //    string valid = string.Format(" \tProcessed {0} line{1} with ", _CNT, (_CNT != 1 ? "s" : "")) +
        //        (errs > 0 ? string.Format("<span style=\"color: tomato\"><b>{0}</b> error{1}</span>", errs, (errs != 1 ? "s" : "")) : "") +
        //        (((errs > 0) && (warns > 0)) ? " and " : "") +
        //        (warns > 0 ? string.Format("<span style=\"color: gray\"><b>{0}</b> warning{1}</span>", warns, (warns != 1 ? "s" : "")) : "");

        //    string use = string.Format(" \tFor full results use the following query on server {0} database{1} {2}:", _SERVER.ToUpper(), _DATABASE.ToUpper(), env);
        //    string sel = string.Format(" \t<span style=\"color: DodgerBlue;\">SELECT * FROM {0} WHERE dvid = <b>{1}</b></span>", rpt_object, dvid);
        //    int len = (new List<string>() { use, use, sel }).Max(s => s.Length);

        //    const string format = " <tr><td>{0}</td>" +
        //        "<td>{1}</td>" +
        //        "<td>{2}</td>" +
        //        "</tr>";

        //    //var grps_w_details = _WARN_LIST.Where(it => rxw.IsMatch(it.Message))
        //    //        .GroupBy(itm => new { itm.FieldName, itm.RuleDefinition, itm.PkValues, itm.FieldValue }).AsEnumerable()
        //    //        //.OrderBy(gr => new { gr.Key })
        //    //        .Select(g => new { Keys = g.Key, FirstKey = g.Key.PkValues.First(), FirstValue=g.Key.FieldValue.First() }).AsEnumerable();

        //    StringBuilder sb = new StringBuilder();
        //    CultureInfo culture = CultureInfo.CurrentCulture;
        //    StringBuilder sb_values = new StringBuilder();
 
        //    sb.AppendLine(" <div style=\"width: 240px; margin: 0 auto\"><p><span style=\"color: Gray \"><hr></span><br/>");
        //    sb.AppendLine(hdr + @"<br/>");
        //    if ((_MAX_ERRORS <= errs + warns) && (_MAX_ERRORS != 0))
        //        sb.AppendLine(string.Format(" \tNumber of errors & warnings reached <i>max_errors limit</i> of {0}<br/>", _MAX_ERRORS));
        //    else
        //    if (errs + warns >= _HARD_STOP)
        //        sb.AppendLine(string.Format(" \tNumber of errors & warnings {0} reached the <i>error_limit</i> of {1}<br/>", errs+ warns, _HARD_STOP));
        //    sb.AppendLine(valid + @"<br/>");
        //    sb.AppendLine(use + @"<br/>");
        //    sb.AppendLine(sel + @"<br/>");
        //    sb.AppendLine(" <p><hr style=\"color: Gray \"></div>");
        //    sb.AppendLine(@"<h3>Summary</h3>");
            
        //    //sb.AppendLine(@" *******************************************************************************************************************************************<br/>");
        //    sb.AppendLine("<div style=\"width: 240px; margin: 0 auto\"><table style=\"width: 100%\">");
        //    sb.AppendLine(" <tr><th style=\"border-bottom: 1px solid gray; text-align: left\">Field Name</th>" +
        //        "<th style=\"border-bottom: 1px solid gray; text-align: left\">Rule or Lookup table</th>" +
        //        "<th style=\"border-bottom: 1px solid gray; text-align: left\">Count</th></tr>");

        //    if (errs > 0)
        //    {
        //        var grps_err = _WARN_LIST.Where(it => rxe.IsMatch(it.Message))
        //            .GroupBy(itm => new { itm.FieldName, itm.RuleDefinition }).AsEnumerable()
        //            //.OrderBy(gr => new { gr.Key })
        //            .Select(g => new { Keys = g.Key, Cnt = g.Count() }).AsEnumerable();
        //        foreach (var grp in grps_err)
        //        {
        //            sb.AppendFormat(culture, format, grp.Keys.FieldName.RemoveQuotes('\'').ToString()
        //                , grp.Keys.RuleDefinition.ToString()//.PadRight(90, ' ')//.Length > 90 ? grp.Keys.RuleDefinition.Substring(0, 87) + " .." : grp.Keys.RuleDefinition   
        //                , string.Format("<span style=\"color: tomato; font - weight:bold\">{0}</span>"
        //                , grp.Cnt.ToString() + " error" + (grp.Cnt != 1 ? "s" : ""))
        //                ).AppendLine();
        //        }                
        //    }
        //    if (warns > 0)
        //    {
        //        var grps_warn = _WARN_LIST.Where(it => rxw.IsMatch(it.Message))
        //            .GroupBy(itm => new { itm.FieldName, itm.RuleDefinition }).AsEnumerable()
        //            //.OrderBy(gr => new { gr.Key })
        //            .Select(g => new { Keys = g.Key, Cnt = g.Count() }).AsEnumerable();
        //        foreach (var grp in grps_warn)
        //        {
        //            sb.AppendFormat(culture, format, grp.Keys.FieldName.RemoveQuotes('\'').ToString()
        //                , grp.Keys.RuleDefinition.ToString()//.PadRight(90, ' ')//.Length > 90 ? grp.Keys.RuleDefinition.Substring(0, 87) + " .." : grp.Keys.RuleDefinition   
        //                , string.Format("<span style=\"color: gray; font - weight:bold\">{0}</span>"
        //                , grp.Cnt.ToString() + " warning" + (grp.Cnt != 1 ? "s" : ""))).AppendLine();
        //        }
        //    }
        //    sb.AppendLine("</table></div>");
        //    //sb.AppendLine(@"<h3>Failed Values</h3><br/>");
 
        //    Console.WriteLine(sb.ToString());
        //    //sb.AppendLine(sb_by_col.ToString());
        //    return  sb.ToString();
 
        //}
        static string FormatWarnEmailBody()
        {
            Regex rxe = new Regex("^['\"]?(E:).*");
            Regex rxw = new Regex("^['\"]?(W:).*");
            string env = (configR._SERVER.ToLower().EndsWith("t") ? "( TEST )" : (configR._SERVER.ToLower().EndsWith("d") ? "( DEV )" : "( PROD )"));
            string dvid = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString();
            string warn_table = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["warn_table"].ToString();
            string rpt_object = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["rpt_object"].ToString();

            long errs = _SUM_LIST.Where(itm => itm.IsError).Select(itm => itm.Cnt).Sum();
            long warns = _SUM_LIST.Where(itm => !(itm.IsError)).Select(itm => itm.Cnt).Sum();
            string hdr = string.Format(" \tFile: <b>{0}</b>", configR._FILE);
            string valid = string.Format(" \tProcessed {0} line{1} with ", _CNT, (_CNT != 1 ? "s" : "")) +
                (errs > 0 ? string.Format("<span style=\"color: tomato\"><b>{0}</b> error{1}</span>", errs, (errs != 1 ? "s" : "")) : "") +
                (((errs > 0) && (warns > 0)) ? " and " : "") +
                (warns > 0 ? string.Format("<span style=\"color: gray\"><b>{0}</b> warning{1}</span>", warns, (warns != 1 ? "s" : "")) : "");

            string use = string.Format(" \tFor full results use the following query on server {0} database{1} {2}:", configR._SERVER.ToUpper(), configR._DATABASE.ToUpper(), env);
            string sel = string.Format(" \t<span style=\"color: DodgerBlue;\">SELECT * FROM {0} WHERE dvid = <b>{1}</b></span>", rpt_object, dvid);
            int len = (new List<string>() { use, use, sel }).Max(s => s.Length);

            const string format = " <tr><td>{0}</td>" +
                "<td>{1}</td>" +
                "<td>{2}</td>" +
                "</tr>";

            //var grps_w_details = _WARN_LIST.Where(it => rxw.IsMatch(it.Message))
            //        .GroupBy(itm => new { itm.FieldName, itm.RuleDefinition, itm.PkValues, itm.FieldValue }).AsEnumerable()
            //        //.OrderBy(gr => new { gr.Key })
            //        .Select(g => new { Keys = g.Key, FirstKey = g.Key.PkValues.First(), FirstValue=g.Key.FieldValue.First() }).AsEnumerable();

            StringBuilder sb = new StringBuilder();
            CultureInfo culture = CultureInfo.CurrentCulture;
            StringBuilder sb_values = new StringBuilder();

            sb.AppendLine(" <div style=\"width: 240px; margin: 0 auto\"><p><span style=\"color: Gray \"><hr></span><br/>");
            sb.AppendLine(hdr + @"<br/>");
            if ((configR._MAX_ERRORS <= errs + warns) && (configR._MAX_ERRORS != 0))
                sb.AppendLine(string.Format(" \tNumber of errors & warnings {0} reached <i>MAX_ERRORS</i> limit of {1}<br/>", errs + warns, configR._MAX_ERRORS));
            else
            if (errs + warns >= configR._HARD_STOP)
                sb.AppendLine(string.Format(" \tNumber of errors & warnings {0} reached <i>ERROR_LIMIT</i> of {1}<br/>", errs + warns, configR._HARD_STOP));
            sb.AppendLine(valid + @"<br/>");
            sb.AppendLine(use + @"<br/>");
            sb.AppendLine(sel + @"<br/>");
            sb.AppendLine(" <p><hr style=\"color: Gray \"></div>");
            sb.AppendLine(@"<h3>Summary</h3>");

            //sb.AppendLine(@" *******************************************************************************************************************************************<br/>");
            sb.AppendLine("<div style=\"width: 240px; margin: 0 auto\"><table style=\"width: 100%\">");
            sb.AppendLine(" <tr><th style=\"border-bottom: 1px solid gray; text-align: left\">Field Name</th>" +
                "<th style=\"border-bottom: 1px solid gray; text-align: left\">Rule or Lookup table</th>" +
                "<th style=\"border-bottom: 1px solid gray; text-align: left\">Count</th></tr>");

            if (errs > 0)
            {
                var grps_err = _SUM_LIST.Where(it => it.IsError).Select(itm => itm);
                foreach (var grp in grps_err)
                {
                    sb.AppendFormat(culture, format, grp.FieldName.RemoveQuotes('\'').ToString()
                        , grp.RuleDefinition.ToString()//.PadRight(90, ' ')//.Length > 90 ? grp.Keys.RuleDefinition.Substring(0, 87) + " .." : grp.Keys.RuleDefinition   
                        , string.Format("<span style=\"color: tomato; font - weight:bold\">{0}</span>"
                        , grp.Cnt.ToString() + " error" + (grp.Cnt != 1 ? "s" : ""))
                        ).AppendLine();
                }
            }
            if (warns > 0)
            {
                var grps_warn = _SUM_LIST.Where(it => !it.IsError).Select(itm => itm);
                  foreach (var grp in grps_warn)
                {
                    sb.AppendFormat(culture, format, grp.FieldName.RemoveQuotes('\'').ToString()
                        , grp.RuleDefinition.ToString()//.PadRight(90, ' ')//.Length > 90 ? grp.Keys.RuleDefinition.Substring(0, 87) + " .." : grp.Keys.RuleDefinition   
                        , string.Format("<span style=\"color: gray; font - weight:bold\">{0}</span>"
                        , grp.Cnt.ToString() + " warning" + (grp.Cnt != 1 ? "s" : ""))).AppendLine();
                }
            }
            sb.AppendLine("</table></div>");
            //sb.AppendLine(@"<h3>Failed Values</h3><br/>");

            Console.WriteLine(sb.ToString());
            //sb.AppendLine(sb_by_col.ToString());
            return sb.ToString();

        }
        static void UpdateWarnList()
        {
            Regex rxe = new Regex("^['\"]?(E:).*");
            //var grps_err = _WARN_LIST
            //    .GroupBy(itm => new { itm.FieldName, itm.RuleDefinition }).AsEnumerable()
            //    .Select(g => new { Keys = g.Key, Cnt = g.Count() }).AsEnumerable();
            foreach (var grp in _WARN_LIST
                .GroupBy(itm => new { itm.FieldName, itm.RuleDefinition, IsErr = rxe.IsMatch(itm.Message) }).AsEnumerable()
                .Select(g => new { Keys = g.Key, Cnt = g.Count() }).AsEnumerable())
                
            {
                if (_SUM_LIST.Where(itm => itm.FieldName == grp.Keys.FieldName 
                && itm.RuleDefinition == grp.Keys.RuleDefinition).Count() > 0)
                    _SUM_LIST.Where(itm => itm.FieldName == grp.Keys.FieldName 
                    && itm.RuleDefinition == grp.Keys.RuleDefinition).ToList()
                    .ForEach(itm => itm.Cnt += grp.Cnt );
                else
                    _SUM_LIST.Add(new SumWarning(grp.Keys.FieldName, grp.Keys.RuleDefinition, grp.Cnt, grp.Keys.IsErr));
            }        
        }
        //static string FormatWarnEmailBody()
        //{
        //    Regex rxe = new Regex("^['\"]?(E:).*");
        //    Regex rxw = new Regex("^['\"]?(W:).*");
        //    int errs = _WARN_LIST.Where(itm => rxe.IsMatch(itm.Message)).Count();
        //    int warns = _WARN_LIST.Where(itm => rxw.IsMatch(itm.Message)).Count();
        //    string hdr = string.Format(" *\tFile {0} validation results: Errors {1} Warnings {2}."
        //        , _FILE.Quotename("\"")
        //        , _WARN_LIST.Where(itm => rxe.IsMatch(itm.Message)).Count()
        //        , _WARN_LIST.Where(itm => rxw.IsMatch(itm.Message)).Count());
        //    string dvid = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["dvid"].ToString();
        //    string warn_table = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["warn_table"].ToString();
        //    string rpt_object = sqlUtils._LocalDS.Tables["FileInfo"].Rows[0]["rpt_object"].ToString();
        //    string res = string.Format(" *\tResults are stored on {2} server \"{1}\" database \"{0}\" table.", warn_table, _DATABASE.ToUpper(), _SERVER.ToUpper());
        //    string use = string.Format(" *\tYou can use the following query for full result set:");
        //    string sel = string.Format(" *\tSELECT * FROM {0} (nolock) WHERE dvid = {1}", rpt_object, dvid);
        //    int len = (new List<string>() { res, use, sel }).Max(s => s.Length);


        //    StringBuilder sb = new StringBuilder();
        //    sb.AppendLine(" **************************************************************************************************");
        //    sb.AppendLine(hdr);
        //    if ((_MAX_ERRORS <= errs + warns) && (_MAX_ERRORS != 0))
        //        sb.AppendLine(string.Format(" *\tNumber of errors & warnings reached MAX_ERRORS limit of {0}", _MAX_ERRORS));
        //    else
        //    if (errs + warns >= _HARD_STOP)
        //        sb.AppendLine(string.Format(" *\tNumber of errors & warnings {0} reached the ERROR_LIMIT of {1}", errs + warns, _HARD_STOP));
        //    sb.AppendLine(res);
        //    sb.AppendLine(use);
        //    sb.AppendLine(sel);
        //    sb.AppendLine(" **************************************************************************************************");
        //    sb.AppendLine();
        //    if (errs > 0)
        //    {
        //        sb.AppendLine(string.Format("\tErrors ({0})", errs));
        //        foreach (FormatWarning warn in _WARN_LIST.Where(itm => rxe.IsMatch(itm.Message)))
        //        {
        //            sb.AppendLine(string.Format("Keys({0})\tField {1}\tValue {2}\t{3}", string.Join(",", warn.PkValues)
        //                , warn.FieldName, warn.FieldValue, warn.Message));
        //        }
        //        sb.AppendLine();
        //    }
        //    if (warns > 0)
        //    {
        //        sb.AppendLine(string.Format("\tWarnings ({0})", warns));
        //        foreach (FormatWarning warn in _WARN_LIST.Where(itm => rxw.IsMatch(itm.Message)))
        //        {
        //            sb.AppendLine(string.Format("Keys({0})\tField {1}\tValue {2}\t{3}", string.Join(",", warn.PkValues)
        //                , warn.FieldName, warn.FieldValue, warn.Message));
        //        }
        //    }
        //    //sb.Clear();

        //    return sb.ToString();
        //}
    }
}

/*
        private static void ErrorOutput(int output, string filename)
        {
            if ((output & 1) == 1) // print to console
            {
                foreach (string s in _ERR_LIST)
                    Console.WriteLine(s);
                //Console.ReadLine();
            }
            if ((output & 2) == 2) // save to file
            {
                using (StreamWriter file = new StreamWriter(_PATH + @"\" + _ERR_LOG, true))
                {
                    foreach (string s in _ERR_LIST)
                        file.WriteLine(s);
                }

            }
            //else if (output == 2) // save to file
            //{
            //    StringBuilder sb = new StringBuilder();
            //    foreach (string s in _ERR_LIST)
            //    {

            //        sb.AppendLine(s);
            //    }
            //    OutputToFile(_PATH + @"/" + filename, sb.ToString());
            //    //                Console.ReadLine();
            //}
            if ((output & 4) == 4) // to table
            {
                foreach (string s in _ERR_LIST)
                    Console.WriteLine(s);
            }
            _ERR_LIST.Clear();


        }
 
 
 */
