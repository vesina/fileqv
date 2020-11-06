using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Text.RegularExpressions;

namespace csvtosql
{
    class ConfigR
    {
        public string _SERVER;
        public string _DATABASE;
        public string _PATH;
        public string _FILE;
        public MetadataSource _METADATA_SOURCE;
        public string _CONFIG_VIEW;
        public string _LOG_TABLE;
        public bool _LOG_ALWAYS;

        public string _SMTP_SERVER;
        public string _SENDER;
        public bool _SEND_EMAIL;
        public string _EMAIL_TO;
        public string _EMAIL_CC;
        public string _EMAIL_BC;
        public string _EMAIL_LIST;
        public ExitCode _EXIT_CODE;
        public ValidationOptions _VALIDATION_OPTIONS;
        public int _BATCH;
        public bool _DEBUG;
        public bool _DEBUG_VERBOSE;
        public int _MAX_ERRORS;
        public bool _SKIP_CHECK_IF_NULL;
        public bool _WARN_ALL_NULLS;
        public bool _QUOTED;
        public DateTime _FILE_DATE;
        public int _OUTPUT;
        public ErrorLevel _ERROR_LEVEL;
        public char _DELIM;
        public int _HARD_STOP;
        public char _QUOTE;
        public int _EMAIL_BATCH;
        public string _OUTPUT_ROWS;
        public string _ERR_LOG;

        public ConfigR ()
        {
            _SERVER = string.Empty;
            _DATABASE = string.Empty;
            _PATH = string.Empty;
            _FILE = string.Empty;
            _METADATA_SOURCE = MetadataSource.Database;
            _CONFIG_VIEW = "DV.vw_configuration";
            _LOG_TABLE = "DV.error_log";
            _LOG_ALWAYS = true;

            _SMTP_SERVER = string.Empty;// "frbgate.frb.gov";
            _SENDER = string.Empty;// "Ramp_FileValidator@frb.gov";
            _SEND_EMAIL = _SMTP_SERVER == string.Empty || _SENDER == string.Empty ? false : true;
            _EMAIL_TO = string.Empty;
            _EMAIL_CC = string.Empty;
            _EMAIL_BC = string.Empty;
            _EMAIL_LIST = string.Empty;
            _EXIT_CODE = ExitCode.None;
            _VALIDATION_OPTIONS = ValidationOptions.ValidateBasic;
            _BATCH = 50000;
            _DEBUG = false;
            _DEBUG_VERBOSE = false;
            _MAX_ERRORS = 0;
            _SKIP_CHECK_IF_NULL = true;
            _WARN_ALL_NULLS = true;
            _QUOTED = true;
            _FILE_DATE = DateTime.MaxValue;
            _OUTPUT = 0;
            _ERROR_LEVEL = ErrorLevel.None;
            _DELIM = ',';
            _HARD_STOP = 5000;
            _QUOTE = '"';
            _EMAIL_BATCH = 1000;
            _OUTPUT_ROWS = @"_out_rows_" + DateTime.Now.ToString("yyyyMMdd--HH-mm-ss") + @".csv";
            _ERR_LOG = "_err_" + DateTime.Now.ToString("yyyyMMdd_HHmmss") + @".csv";
        }

        public string BuildSampleINI()
        {
            StringBuilder sb = new StringBuilder();
            Type t = Type.GetType(this.GetType().ToString());
            FieldInfo[] FieldInfos = t.GetFields();
            sb.AppendLine(string.Format("### Runtime parameters {0} ###", FieldInfos.Count().ToString()));
            sb.AppendLine(string.Format("### Command line input args: PATH, FILE", FieldInfos.Count().ToString()));
            sb.AppendLine(string.Format("### Min required in INI file: SERVER, DATABASE", FieldInfos.Count().ToString())); for (int i = 0; i < FieldInfos.Length; i++)
            {
                try
                {
                    StringBuilder lsb = new StringBuilder();
                    bool is_enum = Type.GetType(FieldInfos[i].FieldType.ToString()).IsEnum;
                    if (is_enum)
                    {
                        var enm = Type.GetType(FieldInfos[i].FieldType.ToString()).GetEnumValues();
                        foreach (var itm in enm)
                            lsb.AppendFormat("{0}:{1}; ", itm.GetHashCode().ToString(), itm.ToString());
                    }

                    if ((FieldInfos[i] != null) && (FieldInfos[i].Name.ToString().StartsWith("_")))
                        sb.AppendLine(string.Format("#{0,-25} #({1}{2}) {3}", FieldInfos[i].Name.ToString().Substring(1, FieldInfos[i].Name.Length - 1) + "="
                            , FieldInfos[i].FieldType.ToString()
                            , is_enum ? "/" + Type.GetType(FieldInfos[i].FieldType.ToString()).GetEnumUnderlyingType().ToString() : ""
                            , (is_enum ? lsb.ToString() : "") + "Default:" + FieldInfos[i].GetValue(this).ToString()));
                }
                catch (Exception)
                {
                    if ((FieldInfos[i] != null) && (FieldInfos[i].Name.ToString().StartsWith("_")))
                        sb.AppendLine(string.Format("{0}={1}", FieldInfos[i].Name.ToString(), "NULL"));
                }
            }
            return sb.ToString();
        }
        public string PrintClassFields(bool include_values)
        {
            StringBuilder sb = new StringBuilder();
            Type t = Type.GetType(this.GetType().ToString());
            FieldInfo[] FieldInfos = t.GetFields();
            sb.AppendLine(string.Format("Runtime parameters {0}", FieldInfos.Count().ToString()));
            for (int i = 0; i < FieldInfos.Length; i++)
            {
                try
                {
                    if ((FieldInfos[i] != null) && (FieldInfos[i].Name.ToString().StartsWith("_")))
                        sb.AppendLine(string.Format("#{0}={1}" 
                            , FieldInfos[i].Name.ToString()
                            , include_values ? FieldInfos[i].GetValue(this).ToString() : ""));

                }
                catch (Exception)
                {
                    if ((FieldInfos[i] != null) && (FieldInfos[i].Name.ToString().StartsWith("_")))
                        sb.AppendLine(string.Format("{0}={1}", FieldInfos[i].Name.ToString(), "NULL"));
                }
            }              
            return sb.ToString();
        }
        public void ReadConfigFile(string configfile)
        {
            if (configfile == "")
            {
                string path = System.AppDomain.CurrentDomain.BaseDirectory;
                DirectoryInfo di = new DirectoryInfo(path);
                FileInfo fi = di.GetFiles("*.conf.ini").FirstOrDefault();
                if (fi != null)
                    configfile = fi.FullName;
                else
                    throw new CustomException(string.Format("Configuration file {0} does not exist.", fi.FullName));
            }
            using (StreamReader sr = new StreamReader(configfile))
            {
                string s;
                while ((s = sr.ReadLine()) != null)
                {
                    if (s.ToCharArray().Contains('#'))
                        s = s.Split('#')[0];
                    if (s.Length > 0)
                    {
                        string[] ar = s.Split('=').ToArray();
                        SetValue(ar[0].ToString().ToLower(), ar[1].ToString().ToString()); 
                    }                        
                }
            }
        }
        public void ReadConfigTable()
        {
            ReadConfigTable(_SERVER, _DATABASE, _CONFIG_VIEW);
 
        }
        public void ReadConfigTable(string server, string database, string configtable)
        {
            string connstr = "Data Source=" + server + ";Initial Catalog=" + database + ";Integrated Security=SSPI"
                + ";Connection Timeout=600";

            using (SqlConnection conn = new SqlConnection(connstr))
            {
                SqlCommand cmd = new SqlCommand();
                conn.Open();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = @"sp_executesql";// this.GetSelect();
                cmd.Parameters.Add(new SqlParameter("stmt", SqlDbType.NVarChar));
                StringBuilder sb = new StringBuilder();
                cmd.Parameters[0].Value = string.Format("select conf_id, conf_variable, conf_value, conf_attr, conf_descr " +
                    " from {0}(nolock) " +
                    " where is_active=1"
                    , configtable);
                SqlDataReader dr = cmd.ExecuteReader();
                while (dr.Read())
                {
                    SetValue(dr["conf_variable"].ToString().ToLower(), dr["conf_value"].ToString());
                }
            }
        }
        public string SourceFileName()
        {
            return _PATH + @"\" + _FILE;// @"\" + ConfigParams["file"].ToString();
        }
        public string ParseCmdParams(string[] cmdargs)
        {
            StringBuilder sb = new StringBuilder("");
            string filename;
            if ((cmdargs.Length == 0))
            {
                if (!_DEBUG)
                    return "Please provide parameter(s): < PATH >,< file.dat > ";
                Console.WriteLine("Please provide parameter(s): <PATH>,<file.dat>");
                string s = Console.ReadLine();
                if ((s != string.Empty))// || (s.ToLower() == "h") || (s == "?") || !(s.Contains(','))
                {
                    return ParseCmdParams(s.Split(',').ToArray());
                }
                else
                    return "Please provide parameter(s): < PATH >,< file.dat >";
            }

            if (cmdargs.Length <= 2)
            {
                if (cmdargs.Length == 1)
                {
                    string arg = cmdargs[0].ToString();
                    if (arg.ToCharArray().Contains(','))
                    {
                        _PATH = arg.Split(',').ToArray()[0];
                        filename = arg.Split(',').ToArray()[1];
                    }
                    else
                    {
                        switch (arg.ToLower())
                        {
                            case "-h":
                            case "--h":
                            case "/h":
                            case "help":
                            case "h":
                                sb.AppendLine(AssemblyInfo.HelpAssemblyInfo());
                                sb.AppendLine(AssemblyInfo.Features);
                                break;
                            case "-?":
                            case "--?":
                            case "/?":
                            case "?":
                                sb.AppendLine(AssemblyInfo.HelpAssemblyInfo());
                                break;
                            case "v":
                            case "ver":
                            case "version":
                            case "/v":
                            case "-v":
                            case "-ver":
                            case "-version":
                            case "--v":
                            case "--ver":
                            case "--version":
                                sb.AppendLine(AssemblyInfo.VersionHistory);
                                sb.AppendLine(AssemblyInfo.HelpAssemblyInfo());
                                break;
                            case "c":
                            case "conf":
                            case "config":
                            case "configuration":
                            case "/c":
                            case "-c":
                            case "--c":
                                sb.AppendLine(PrintClassFields(true));
                                break;
                            case "ini":
                            case "validator.conf.ini":
                                sb.AppendLine(BuildSampleINI());
                                string file = Guid.NewGuid().ToString() + ".ini";
                                //Program.OutputToFile(file, sb.ToString());

                                break;
                            default:

                                break;
                        }
                        return sb.ToString();
                    }                    
                }
                else //                if (cmdargs.Length == 2)
                {
                    _PATH = cmdargs[0].ToString();
                    filename = cmdargs[1].ToString();
                }
                
                if ((filename == null) || (filename == "") || (!File.Exists(_PATH + @"\" + filename)))
                {
                    sb.AppendLine(string.Format("File \"{0}\" does not exist.", _PATH + @"\" + filename));
                    return sb.ToString();
                }

                FileInfo fi = new FileInfo(_PATH + @"\" + filename);

                if (fi.Extension.ToLower() == ".dat")
                {
                    try
                    {
                        using (StreamReader sr = new StreamReader(_PATH + @"\" + filename))
                        {
                            _FILE = sr.ReadLine();
                        }
                    }
                    catch (Exception ex)
                    {
                        sb.AppendLine(string.Format("Cannot read file \"{0}\".\n{1}", filename, ex.Message.ToString()));
                        //throw new CustomException(string.Format("Cannot read file {0}.\n{1}", filename, ex.Message.ToString()));
                    }

                }
                else
                    _FILE = filename;
                
                if (!File.Exists(SourceFileName()))
                {
                    sb.AppendLine(string.Format("Source data file {0} does not exist.", SourceFileName()));
                    //throw new CustomException(string.Format("Source data file {0} does not exist.", SourceFileName()));
                }
 
                _FILE_DATE = File.GetCreationTime(SourceFileName());
                return sb.ToString();
            }
            else
            /// if more than 2 arguments - parse them to params
            try
            {
                foreach (string arg in cmdargs)
                {
                    switch (arg.ToCharArray()[0])
                    {
                        case 's':
                            _SERVER = arg.Replace("s:", "");
                            break;
                        case 'd':
                            _DATABASE = arg.Replace("d:", "");
                            break;
                        case 'p':
                            _PATH = arg.Replace("p:", "");
                            break;
                        case 'f':
                            _FILE = arg.Replace("f:", "");
                            break;
                        case 'b':
                            _BATCH = Int32.Parse(arg.Replace("b:", ""));
                            break;
                        case 'g':
                            _DEBUG = Boolean.Parse(arg.Replace("g:", ""));
                            break;
                        case 'l':
                            _DELIM = Char.Parse(arg.Replace("l:", ""));
                            break;
                        case 'q':
                            _QUOTE = Char.Parse(arg.Replace("q:", ""));
                            break;
                        case 'o':
                            _OUTPUT = Int32.Parse(arg.Replace("o:", ""));
                            break;
                        default:

                            break;
                    }
                }
                return "";
            }
            catch(Exception ex)
            {
                return string.Format("Cannot read input parameters.\n{0}", ex.Message.ToString());
            }
        }

        private void SetValue(string k, string value)
        { 
            switch (k.ToLower())
            {
                case "server":
                    _SERVER = value;
                    break;
                case "database":
                    _DATABASE = value;
                    break;
                case "path":
                    _PATH = value;
                    break;
                case "file":
                    _FILE = value;
                    break;
                case "metadata_source":
                    _METADATA_SOURCE = (MetadataSource)(int.Parse(value.StringTrim()));
                    break;
                case "config_view":
                    _CONFIG_VIEW = value;
                    break;
                case "log_table":
                    _LOG_TABLE = value;
                    break;
                case "log_always":
                    _LOG_ALWAYS = value.ToBool();
                    break;
                case "smtp_server":
                    _SMTP_SERVER = value;
                    break;
                case "sender":
                    _SENDER = value;
                    break;
                case "send_email":
                    _SEND_EMAIL = value.ToBool();
                    break;
                case "email_to":
                    _EMAIL_TO = value;
                    break;
                case "email_cc":
                    _EMAIL_CC = value;
                    break;
                case "email_bc":
                    _EMAIL_BC =  value;
                    break;
                case "validation_options":
                case "options":
                    _VALIDATION_OPTIONS = (ValidationOptions)(int.Parse(value));
                    break;
                case "batch":
                    _BATCH = int.Parse(value);
                    break;
                case "debug":
                    _DEBUG = value.ToBool();
                    break;
                case "debug_level":
                    _DEBUG_VERBOSE = value.ToBool();
                    break;
                case "max_errors":
                    _MAX_ERRORS = int.Parse(value);
                    break;
                case "skip_check_if_null":
                    _SKIP_CHECK_IF_NULL = value.ToBool();
                    break;
                case "warn_all_nulls":
                    _WARN_ALL_NULLS = value.ToBool();
                    break;
                case "quoted":
                    _QUOTED = value.ToBool();
                    break;
                case "file_date":
                    _FILE_DATE = value.ToDate();
                    break;
                case "output":
                    _OUTPUT = int.Parse(value);
                    break;
                case "error_level":
                    _ERROR_LEVEL = (ErrorLevel)(int.Parse(value));
                    break;
                case "delim":
                case "delimiter":
                    _DELIM = Char.Parse(value);
                    break;
                case "hard_stop":
                    _HARD_STOP = int.Parse(value);
                    break;
                case "email_batch":
                    _EMAIL_BATCH = int.Parse(value);
                    break;

                case "exit_code":
                    _EXIT_CODE = (ExitCode)(int.Parse(value));
                    break;
            }
        }
    }
}
